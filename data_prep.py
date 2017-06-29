# This script performs all steps of processing InfoGroup data, from original raw CSV files to BigQuery database.
# Each step is done by a separate function, and run(test) runs all steps in order.
# run(True) will perform test run on small portion of raw data. All paths will be prepended with /test/.
# Step is skipped if it's expected output file/blob already exists. This is a way to resume previously interrupted code without repeating same steps again.
# Code uses ipyparallel and should be run after launching "ipcluster start -nX"
# Most steps take data year as argument, and execution is parallelized over years.




import json, csv, os, io, uuid, time
import ipyparallel as ipp
from google.cloud import storage, bigquery

# Global constants
years = list(range(1997, 2016))
bucket_orig = 'info_group_original'
blob_orig = '{y}_Business_Academic_QCQ.csv'
bucket_corr = 'info-group-corr'
blob_corr = '{y}.csv'
blob_err = 'err/{y}.csv'
path_orig = 'orig/{y}.csv'
path_conv = 'conv/{y}.csv'
path_schema = 'schema.json'
path_err = 'err/{y}.json'
path_corr = 'corr/{y}.csv'
bq_dataset = 'original'
bq_table = 'data'

def make_locations():
    print('Create folders...', end=' ')
    _make_dir(path_orig)
    _make_dir(path_conv)
    _make_dir(path_err)
    _make_dir(path_corr)
    print('finished.')

    
def _make_dir(file_path):
    d = os.path.dirname(file_path)
    if not os.path.exists(d):
        os.makedirs(d)
    
def make_schema(y):
    '''Prepare year-specific schema.'''
    with open(path_schema, 'r') as f:
        schema = json.load(f)
    for fl in schema['field_lists']:
        if y in fl['years']:
            used_fields = fl['fields']
            break
    new_fields = []
    for f in schema['fields']:
        if f['name'] in used_fields:
            new_fields.append(f)
    schema['fields'] = new_fields
    return schema
    
def get_orig_data(y):
    '''Download original data from GC Storage to local hard drive.'''
    dst = path_orig.format(y=y)
    if os.path.exists(dst):
        print('get_orig_data: {} already exists'.format(dst))
        return

    print('get_orig_data: {y} started'.format(y=y))
    client = storage.Client()
    bucket = client.get_bucket(bucket_orig)
    blob = bucket.blob(blob_orig.format(y=y))
    blob.download_to_filename(dst)
    print('get_orig_data: {y} finished'.format(y=y))

def convert(y):
    '''
    Convert original data to UTF-8.
    Remove double quotes.
    Change variable names in header row.
    Assert that year in file name is the same as "Archive Version Year".
    Change format of numbers in 2009: pad with leading zeros.
    '''
    
    dst = path_conv.format(y=y)
    if os.path.exists(dst):
        print('convert: {} already exists'.format(dst))
        return
    
    
    print('convert: {y} started'.format(y=y))

    # Prepare map of renames
    with open(path_schema, 'r') as f:
        schema = json.load(f)
    field_new_names = {}
    for f in schema['fields']:
        field_new_names[f['originalName']] = f['name']

    # Special rules for 2009
    if y == 2009:
        format_2009 = [
            {'field': 'zip', 'width': 5},
            {'field': 'zip4', 'width': 4},
            {'field': 'county_code', 'width': 3},
            {'field': 'sic', 'width': 6},
            {'field': 'sic0', 'width': 6},
            {'field': 'sic1', 'width': 6},
            {'field': 'sic2', 'width': 6},
            {'field': 'sic3', 'width': 6},
            {'field': 'sic4', 'width': 6},
            {'field': 'yp_code', 'width': 5},
            {'field': 'abi', 'width': 9},
            {'field': 'subsidiary_number', 'width': 9},
            {'field': 'parent_number', 'width': 9},
            {'field': 'site_number', 'width': 9},
            {'field': 'census_tract', 'width': 6},
            {'field': 'census_tract', 'width': 6},
            {'field': 'census_tract', 'width': 6},
            {'field': 'census_tract', 'width': 6},
            {'field': 'cbsa_code', 'width': 5},
            {'field': 'csa_code', 'width': 3},
            {'field': 'fips_code', 'width': 5}
        ]

    # Convert
    src = path_orig.format(y=y)
    with open(src, mode='r', encoding='us-ascii', errors='ignore', newline='') as fi, \
         open(dst, mode='w', encoding='utf-8', newline='') as fo:
        reader = csv.reader(fi)
        writer = csv.writer(fo)

        # header
        field_names = next(reader)
        assert all([f in field_new_names for f in field_names])
        y_i = field_names.index('Archive Version Year')
        field_names = [field_new_names[f] for f in field_names]
        writer.writerow(field_names)
        if y == 2009:
            for f9 in format_2009:
                f9['idx'] = field_names.index(f9['field'])

        # data rows
        for row in reader:
            assert y == int(row[y_i])
            if y == 2009:
                for f9 in format_2009:
                    idx = f9['idx']
                    val = row[idx]
                    if val != '':
                        wid = f9['width']
                        row[idx] = '{:0{width}d}'.format(int(val), width=wid)

            writer.writerow(row)
            
    print('convert: {y} finished'.format(y=y))
    
def validate(y):
    '''
    Validate converted CSV files against schema.
    Errors are saved into JSON file. Column and row numbers are 1-based.
    '''
    dst = path_err.format(y=y)
    if os.path.exists(dst):
        print('validate: {} already exists'.format(dst))
        return
    
    print('validate: {y} started'.format(y=y))

    
    # Validate
    inspector = Inspector(row_limit=10**8)
    fn = path_conv.format(y=y)
    report = inspector.inspect(fn, 'table', schema=make_schema(y))
    err_count = report['error-count']
    print('{y} errors: {n}.'.format(y=y, n=err_count))
    
    # Save errors
    with open(dst, 'w') as f:
        json.dump(report['tables'][0]['errors'], f, indent=2)
    
    print('validate: {y} finished'.format(y=y))


def print_errors(y):
    with open(path_err.format(y=y), 'r') as f:
        errors = json.load(f)
    err_count = len(errors)
    print('Errors in {y}: {n}'.format(y=y, n=err_count))
    if err_count > 0:
        schema = make_schema(y)
        field_names = [f['name'] for f in schema['fields']]
    for e in errors:
        col = e['column-number']
        name = field_names[col - 1]
        msg = e['message']
        print(name + ': ' + msg)
        

def correct(y):
    '''
    Correct CSV files according to errors reported by validation.
    All wrong values are overwritten with '' (null).
    Files with errors are copied with corrections, files without errors are moved.
    '''

    src = path_conv.format(y=y)
    dst = path_corr.format(y=y)
    if os.path.exists(dst):
        print('correct: {} already exists'.format(dst))
        return
    
    with open(path_err.format(y=y), 'r') as f:
        errors = json.load(f)
    if len(errors) == 0:
        print('correct: {} has 0 errors'.format(src))
        return
    
    print('correct: {y} started'.format(y=y))
    
    # Two conversion functions are used, so only lines with errors are parsed
    def csv_from_str(text):
        return next(csv.reader([text]))

    def csv_to_str(values):
        out = io.StringIO()
        csv.writer(out, lineterminator='\n').writerow(values)
        return out.getvalue()    
    

    errors.sort(key=lambda x: x['row-number'])

    with open(src, 'r') as fi, open(dst, 'w') as fo:

        # transform to 0-based
        next_err_row = errors[0]['row-number'] - 1

        for row_i, row in enumerate(fi): 
            if row_i == next_err_row:
                values = csv_from_str(row)
                # fix all errors in current row
                while len(errors) > 0 and errors[0]['row-number'] - 1 == row_i:
                    err = errors.pop(0)
                    col_i = err['column-number'] - 1 # to 0-based
                    values[col_i] = ''
                row = csv_to_str(values)
                if len(errors) > 0:
                    next_err_row = errors[0]['row-number'] - 1

            fo.write(row)
        
    print('correct: {y} finished'.format(y=y))
    

def upload_corr_to_storage(y):
    '''
    Upload corrected files to GC Storage.
    '''
    
    client = storage.Client()
    bucket = client.bucket(bucket_corr)
    dst = blob_corr.format(y=y)
    blob = bucket.blob(dst)
    if blob.exists():
        print('upload_corr_to_storage: blob {} already exists'.format(dst))
        return
    
    print('upload_corr_to_storage: {y} started'.format(y=y))
    
    with open(path_err.format(y=y), 'r') as f:
        errors = json.load(f)
    if len(errors) == 0:
        src = path_conv.format(y=y)
    else:
        src = path_corr.format(y=y)

    print('upload_corr_to_storage: {}'.format(src))
    blob.upload_from_filename(src)
    
    print('upload_corr_to_storage: {y} finished'.format(y=y))
    
    
def upload_err_to_storage(y):
    '''
    Upload error report files to GC Storage.
    '''
    
    client = storage.Client()
    bucket = client.bucket(bucket_corr)
    dst = blob_err.format(y=y)
    blob = bucket.blob(dst)
    if blob.exists():
        print('upload_err_to_storage: blob {} already exists'.format(dst))
        return
    
    print('upload_err_to_storage: {y} started'.format(y=y))

    src = path_err.format(y=y)
    blob.upload_from_filename(src)
    
    print('upload_err_to_storage: {y} finished'.format(y=y))
    
    
def upload_to_bq():
    '''
    Create BQ table and upload CSV files from GC Storage.
    '''
    client = bigquery.Client()
    dataset = client.dataset(bq_dataset)
    table = dataset.table(bq_table)
    if table.exists():
        print('upload_to_bq: table already exists')
        return

    def make_bq_schema(y):
        'Create BQ schema.'

        schema = make_schema(y)
        
        # Conversion table of JSON Schema types to BigQuery types
        bq_type = {
            'integer': 'INTEGER',
            'string': 'STRING',
            'year': 'INTEGER',
            'number': 'FLOAT'
        }
        
        bq_schema = []
        for f in schema['fields']:
            bq_schema.append(bigquery.SchemaField(f['name'], bq_type[f['type']], description=f['originalDescription']))

        return bq_schema

    bq_schema_full = make_bq_schema(1999) # all fields
    table = dataset.table(bq_table, schema=bq_schema_full)
    table.create()
    table.description = 'All years in one table.\n'
    table.description += 'In years where certain fields do not exist, corresponding values are NULL.'
    table.update()

    # Start upload jobs
    jobs = {}
    for y in years:
        job_name = str(uuid.uuid4())
        source = 'gs://{}/{}'.format(bucket_corr, blob_corr.format(y=y))
        job = client.load_table_from_storage(job_name, table, source)
        job.skip_leading_rows = 1
        job.source_format = 'CSV'
        job.schema = make_bq_schema(y)
        job.create_disposition = 'CREATE_NEVER'
        job.write_disposition = 'WRITE_APPEND'
        job.begin()
        jobs[y] = job
        
    print('upload_to_bq: all jobs started')
        
    # wait until all jobs finish
    while len(jobs) > 0:
        done_years = []
        for y, job in jobs.items():
            job.reload()
            if job.state == 'DONE':
                done_years.append(y)
                print('upload_to_bq: {y} finished'.format(y=y))
                if job.errors is not None:
                    print(job.errors)
        for y in done_years:
            del jobs[y]
        time.sleep(1)
    
    print('upload_to_bq: all finished'.format(y=y))

def test_add_errors():
    '''Add errors for testing purposes.'''
    src = path_conv.format(y=1997)
    dst = src + '_'
    with open(src, 'r') as fi, open(dst, 'w') as fo:
        for i, l in enumerate(fi):
            if i == 1:
                fo.write('DEVELOPMENT ASSOCIATES,630 SILVER ST # 3C,AGAWAM,MA,01001,2987,013,413,,A,B,655202,REAL ESTATE DEVELOPERS,23721005,LAND SUBDIVISION,,,,,,,,,,,1997,,3,600,,,,,0,@316162510,,,,,,,,,1,813205,2,042.05636,-072.6512,P,44140,2,521,25013\n')
            elif i == 2:
                fo.write('H P HOOD INC,233 MAIN ST,AGAWAM,MA,01001,1882,013,413,,F,I,024101,DAIRIES (MILK),01212001,DAIRY CATTLE & MILK PRODUCTION,024101,DAIRIES (MILK),,,,,,,,,1997,,170,124270,2,,,,0,009617762,000501056,003434610,,,,,,,1,813209,1,042.08605,-072.6174,P,44140,2,521,25013\n')
            else:
                fo.write(l)
                
    os.replace(dst, src)
    

def run(test):
    '''Do full run'''
    # overwrite global path variables
    global blob_orig, blob_corr, blob_err, path_orig, path_conv, path_err, path_corr, bq_table
    if test:
        blob_orig = 'test/{y}_Business_Academic_QCQ.csv'
        blob_corr = 'test/{y}.csv'
        blob_err = 'test/err/{y}.csv'
        path_orig = 'test/orig/{y}.csv'
        path_conv = 'test/conv/{y}.csv'
        path_err = 'test/err/{y}.json'
        path_corr = 'test/corr/{y}.csv'
        bq_table = 'test_data'        
    
    make_locations()

    rc = ipp.Client()
    dview = rc[:]
    lview = rc.load_balanced_view()

    dview.push(dict(
        bucket_orig=bucket_orig,
        blob_orig=blob_orig,
        bucket_corr=bucket_corr,
        blob_corr=blob_corr,
        blob_err=blob_err,
        path_orig=path_orig,
        path_conv=path_conv,
        path_schema=path_schema,
        path_err=path_err,
        path_corr=path_corr,
        make_schema=make_schema))

    dview.execute('from google.cloud import storage')
    dview.execute('import os, json, csv, io')
    dview.execute('from goodtables import Inspector')

    # download
    print(get_orig_data.__doc__)
    res = lview.map_async(get_orig_data, years)
    res.wait_interactive()
    res.display_outputs()

    # convert
    print(convert.__doc__)
    res = lview.map_async(convert, years)
    res.wait_interactive()
    res.display_outputs()   

    if test:
        test_add_errors()

    # validate
    print(validate.__doc__)
    res = lview.map_async(validate, years)
    res.wait_interactive()
    res.display_outputs()

    # report errors
    for y in years:
        print_errors(y)

    # correct
    print(correct.__doc__)
    res = lview.map_async(correct, years)
    res.wait_interactive()
    res.display_outputs()   

    # upload corrected files to storage
    print(upload_corr_to_storage.__doc__)
    res = lview.map_async(upload_corr_to_storage, years)
    res.wait_interactive()
    res.display_outputs()
    
    # upload error reports to storage
    print(upload_err_to_storage.__doc__)
    res = lview.map_async(upload_err_to_storage, years)
    res.wait_interactive()
    res.display_outputs()
    
    # upload to bq
    upload_to_bq()
    
    
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--full', help='perform full run (test run otherwise)', action='store_true')
    args = parser.parse_args()
    if args.full:
        test_run = False
    else:
        test_run = True
    run(test_run)