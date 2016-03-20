import boto
import csv


def progress(sofar, total):
    print 'Read %d bytes out of %d' % (sofar, total)

    
class AwsBilling(object):
    def __init__(self, config_csv):
        with open(config_csv, 'rb') as config:
            reader = csv.DictReader(config)
            row = reader.next()
            self.s3 = boto.connect_s3(row['key_id'], row['key'])
            self.bucket_name = row['bucket_name']
    
    def get_billing_file(self):
        bucket = self.s3.get_bucket(self.bucket_name)
        csv_zip_name = None
        for key in bucket.list():
            if key.key[-7:] == 'csv.zip':
                csv_zip_name = key.key
                with open(key.key, 'wb') as csv_zip:
                    key.get_file(csv_zip, cb=progress)
                break
        self.s3.close()

        import zipfile

        if zipfile.is_zipfile(csv_zip_name):
            archive = zipfile.ZipFile(csv_zip_name)
            for item in archive.infolist():
                archive.extract(item)
                
        self.csv_name = item.filename
        return self
        
    def analyze(self):
        with open(self.csv_name, 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['RecordType'] == 'StatementTotal':
                    print "%s: $%s\n" % (row['ItemDescription'], row['Cost'])

                
if __name__ == '__main__':
    AwsBilling('config.csv').get_billing_file().analyze()
