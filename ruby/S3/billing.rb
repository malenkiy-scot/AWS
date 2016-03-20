require "aws-sdk-core"
require "csv"
require "openssl"

class AwsBilling
    def initialize(config_csv)
        # read credentials, region, and other info from the config file
        @config = CSV.table(config_csv)[0]

        creds = Aws::Credentials.new(@config[:key_id], @config[:key])
        @s3 = Aws::S3::Client.new(credentials: creds, region: @config[:region])
    end
    
    def get_billing_file
        csv_zip_name=nil
        bucket_name = @config[:bucket_name]

        resp = @s3.list_objects(bucket: bucket_name)
        resp.contents.each do |object|
            if object.key[-7..-1] == 'csv.zip'
                csv_zip_name = object.key
                break
            end
        end

        @s3.get_object(
            response_target: csv_zip_name,
            bucket: bucket_name,
            key: csv_zip_name
        )

        require "zip"

        @csv_name=csv_zip_name[0..-5]

        zip_file = Zip::ZipFile.open(csv_zip_name)
        zip_file.extract(@csv_name,@csv_name) {true}
        
        return self
    end
    
    def analyse
        billing = CSV.table(@csv_name)
        totals = billing.select {|row| row[:recordtype] == 'StatementTotal'}

        totals.each do |row|
            print "#{row[:itemdescription]}: $#{row[:cost]}\n"
        end
    end
end

# the following overrides SSL verification;
# not good, but otherwise the code does not work
# TODO: need to find a better solution
OpenSSL::SSL::VERIFY_PEER = OpenSSL::SSL::VERIFY_NONE

AwsBilling.new('config.csv').get_billing_file.analyse
