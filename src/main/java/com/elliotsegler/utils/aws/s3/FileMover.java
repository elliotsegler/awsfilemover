package com.elliotsegler.utils.aws.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.codec.binary.Base64InputStream;

import java.io.File;
import java.io.FileInputStream;

/**
 * Created by esegler on 25/02/15.
 */

public class FileMover {

    private String accessKey = null;
    private String secretKey = null;
    private String regionName = "us-west-2";
    private String bucketName = null;
    private String filePath = null;
    private String keyPrefix = null;
    private Boolean overwrite = false;
    private Boolean dryRun = false;
    private String renameFile = null;

    private AWSCredentials awsCredentials = null;
    private AmazonS3Client s3 = null;

    public FileMover(String[] args) {

        parseArgs(args);

        // Create a new AmazonS3 client
        try {
            this.awsCredentials = setBasicCredentials();
        } catch (Exception ex) {
            System.err.println("Error setting basic credentials: " + ex.getMessage());
            usage();
        }

        this.s3 = new AmazonS3Client();
        Region region = null;
        try {
            region = Region.getRegion(Regions.fromName(regionName));
        } catch (Exception ex) {
            System.err.println("Error setting region: " + ex.getMessage());
            usage();
        }

        s3.setRegion(region);

        try {
            String bucketLocation = s3.getBucketLocation(bucketName);
        } catch (Exception ex) {
            System.err.println("Error getting bucket: " + this.bucketName);
            System.err.println(ex.getMessage());
        }

        try {
            File file = new File(filePath);
            if (file.isDirectory()) {
                showFiles(file.listFiles());
            } else if (file.isFile()) {
                uploadFileToS3(file);
            } else {
                System.err.println("Error reading file: " + filePath);
                usage();
            }
        } catch (Exception ex) {
            System.err.println("Error reading file: " + ex.getMessage());
        }

    }

    private void uploadFileToS3(File file) {
        if (this.s3 == null) {
            System.err.println("S3Client not initalized...");
            System.exit(99);
        }

        // Work out the path relative to our starting point
        // If it's equal, then use the name otherwise truncate it to a relative path
        String key = (file.getPath() == this.filePath ? file.getName() : file.getPath().substring(this.filePath.length()));
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        key = (this.keyPrefix == null ? key : this.keyPrefix + "/" + key);

        // If not in overwrite mode, check to see if the key exists
        try {
            if (!this.overwrite) {
                S3Object s3Object = s3.getObject(this.bucketName, key);
                if (s3Object != null) {
                    System.out.println("Key exists for: " + key);
                    return;
                }
            }
        } catch (AmazonS3Exception s3ex) {
            // Do nothing...
        }

        try {

            FileInputStream fileInputStream = new FileInputStream(file);
            Base64InputStream base64InputStream = new Base64InputStream(fileInputStream, true);

            // Create a new ObjectMetadata object to set the content to encrypted
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            objectMetadata.setContentEncoding("base64");

            // Create a new PutObjectRequest to prepare for S3
            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(this.bucketName, key, base64InputStream, objectMetadata);
            //PutObjectRequest putObjectRequest = new PutObjectRequest(this.bucketName, key, file);

            // Upload the file
            if (!this.dryRun) {
                PutObjectResult result = s3.putObject(putObjectRequest);

                if (result == null) {
                    System.err.println("PutObjectRequest for key: " + key + " with file: " + file.getPath() + " failed");
                }

                if (this.renameFile != null) {
                    File newFile = new File(file.getPath() + this.renameFile);
                    file.renameTo(newFile);
                    newFile = null;
                }

                System.out.println("PutObjectRequest for key: " + key + " with file: " + file.getPath() + " succeeded");

                result = null;
                file = null;

            } else {
                System.out.println("DRYRUN - Would upload file: " + file + " to key: " + key);
            }

            fileInputStream = null;
            base64InputStream = null;
            objectMetadata = null;
            putObjectRequest = null;

        } catch (Exception ex) {
            System.err.println("An Error occurred: " + ex.getMessage());
            ex.printStackTrace();
            return;
        }
    }

    private void showFiles(File[] files) {
        for (File file : files) {
            if (file.isDirectory()) {
                showFiles(file.listFiles());
            } else {
                uploadFileToS3(file);
            }
        }
    }

    private AWSCredentials setBasicCredentials() {
        if (this.accessKey == null || this.secretKey == null) {
            System.err.println("Both accessKey and secretKey are required in this mode!");
            usage();
        }
        return new BasicAWSCredentials(this.accessKey, this.secretKey);
    }

    public static void main(String[] args) {
        FileMover fileMover = new FileMover(args);
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-region")) {
                this.regionName = args[i + 1];
                i++;
            } else if (args[i].equalsIgnoreCase("-secretKey")) {
                this.secretKey = args[i + 1];
                i++;
            } else if (args[i].equalsIgnoreCase("-accessKey")) {
                this.accessKey = args[i + 1];
                i++;
            } else if (args[i].equalsIgnoreCase("-bucket")) {
                this.bucketName = args[i + 1];
                i++;
            } else if (args[i].equalsIgnoreCase("-file")) {
                this.filePath = args[i + 1];
                i++;
            } else if (args[i].equalsIgnoreCase("-keyPrefix")) {
                this.keyPrefix = args[i + 1];
                i++;
            }  else if (args[i].equalsIgnoreCase("-renameFile")) {
                this.renameFile = args[i + 1];
                i++;
            } else if (args[i].equalsIgnoreCase("-overwrite")) {
                this.overwrite = true;
                i++;
            } else if (args[i].equalsIgnoreCase("-dryRun")) {
                this.dryRun = true;
                i++;
            } else {
                System.err.println("Invalid argument entered: " + args[i]);
                usage();
            }
        }
    }

    private void usage() {
        System.err.println("\nUsage: java -jar FileMover.jar [options]");
        System.err.println("");
        System.err.println("    where options are:");
        System.err.println("");
        System.err.println("    -region <region name>       - AWS Region name, default is us-west-2");
        System.err.println("    -accessKey <AccessKeyID>    - AWS Access Key ID, mandatory");
        System.err.println("    -secretKey <SecretKey>      - AWS Secret Key, mandatory");
        System.err.println("    -bucket <bucketName>        - Bucket to use, mandatory");
        System.err.println("    -file <path>                - Path to file or directory to move, mandatory");
        System.err.println("    -keyPrefix <prefix>         - Optional prefix to keys/files uploaded");
        System.err.println("    -overwrite                  - Optional flag to overwrite keys that already exist");
        System.err.println("    -dryRun                     - Log only, don't actually upload files");
        System.exit(-1);
    }

}
