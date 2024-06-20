const AWS = require('aws-sdk');
const { Storage } = require('@google-cloud/storage');
const fs = require('fs');
const path = require('path');

async function awsToGcpMigration(awsAccount, gcpAccount, bucketName, destinationBucketName) {
  // Configure AWS S3
  AWS.config.update({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION
  });
  const s3 = new AWS.S3();

  // Configure Google Cloud Storage
  const storage = new Storage({ keyFilename: gcpAccount });
  const gcsBucket = storage.bucket(destinationBucketName);

  // List objects in the AWS S3 bucket
  const objects = await s3.listObjectsV2({ Bucket: bucketName }).promise();
  
  if (!objects.Contents || objects.Contents.length === 0) {
    console.log(`No objects found in the S3 bucket: ${bucketName}`);
    return;
  }

  for (const obj of objects.Contents) {
    const key = obj.Key;
    const localFilePath = path.join(__dirname, key);
    
    console.log(`Processing ${key}`);

    // Download the object from S3
    const params = { Bucket: bucketName, Key: key };
    const data = await s3.getObject(params).promise();
    
    // Write the object data to a local file
    fs.writeFileSync(localFilePath, data.Body);

    // Upload the object to Google Cloud Storage
    await gcsBucket.upload(localFilePath, {
      destination: key
    });

    // Optionally, delete the local file after upload
    fs.unlinkSync(localFilePath);

    console.log(`Successfully migrated ${key} to ${destinationBucketName}`);
  }
}

// Parse command-line arguments
const [awsAccount, gcpAccount, s3BucketName, gcsBucketName] = process.argv.slice(2);

if (!awsAccount || !gcpAccount || !s3BucketName || !gcsBucketName) {
  console.log("Usage: node momento.js <aws_account> <gcp_account> <s3_bucket_name> <gcs_bucket_name>");
  process.exit(1);
}

// Run the migration
awsToGcpMigration(awsAccount, gcpAccount, s3BucketName, gcsBucketName)
  .then(() => console.log("Migration completed successfully"))
  .catch(err => console.error("Migration failed", err));
