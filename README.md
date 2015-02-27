# awsfilemover
Tool to move files from disk to S3

Will build to a fat jar

### Usage

    Usage: java -jar FileMover.jar [options]
    where options are:

    -region <region name>       - AWS Region name, default is us-west-2
    -accessKey <AccessKeyID>    - AWS Access Key ID, mandatory
    -secretKey <SecretKey>      - AWS Secret Key, mandatory
    -bucket <bucketName>        - Bucket to use, mandatory
    -file <path>                - Path to file or directory to move, mandatory
    -keyPrefix <prefix>         - Optional prefix to keys/files uploaded
    -overwrite                  - Optional flag to overwrite keys that already exist
    -dryRun                     - Log only, don't actually upload files
