import {
    S3Client,
    PutObjectCommand,
    GetObjectCommand,
  } from "@aws-sdk/client-s3"
import { exec } from 'child_process';
import { readFileSync, writeFileSync, unlinkSync,existsSync } from 'fs';
import path, { basename } from 'path';

const s3Client = new S3Client({ region: 'us-east-1' });


export async function handler(event) {
    const bucket = event.Records[0].s3.bucket.name;
    let key = decodeURIComponent(
      event.Records[0].s3.object.key.replace(/\+/g, " ")
    );
    console.log("hello! processing new s3 file!", key);
    try {
    //key looks like 139647_Tes/126282_Luk/NIV11-LUK-001-001004v01.mp3
    const ext = path.extname(key);
      
    if (ext === ".key") {
        const obj = await s3Client.send(
            new GetObjectCommand({ Bucket: bucket, Key: key }),
          );

        const data = await obj.Body.transformToString();
        var json = JSON.parse(data);
        const id = json.id;
        const inputKey = json.inputKey;
        const ext = path.extname(inputKey);
        const inputFile = `${id}_input${ext}`;
        const inputPath = `${process.env.efspath}/${inputFile}`;
        await downloadFileFromS3(json.inputBucket, inputKey, inputPath);
      
        console.log(`${inputPath} downloaded`,  existsSync(inputPath));

        const outputFile = `${id}_${basename(json.outputKey)}`; 

        let outputFilePath = `${process.env.efspath}/${outputFile}`;
        if  (existsSync(outputFilePath)) {
          unlinkSync(outputFilePath);
        }
        //let command = `/opt/bin/ffmpeg -i "${json.s3signedurl}"  -acodec libmp3lame -q:a 2 -metadata title="my title" -f mpegts ${outputFilePath}`;        //pipe:`;  
        
      
        let command = `/opt/bin/ffmpeg -i ${inputPath} -acodec libmp3lame -q:a 2 -f mp3 ${outputFilePath}`; 
        await execPromise(command);


        // Step 3: Upload the processed file back to S3
        await uploadFileToS3(json.outputBucket, json.outputKey,
          outputFilePath);
        
          unlinkSync(inputPath);
          unlinkSync(outputFilePath);
        
        return {
            statusCode: 200,
            body: `https://${json.outputBucket}.s3.amazonaws.com/${json.outputKey}`
        };
        
    }
    } catch (error) {
        console.log(error);
        return {
            statusCode: 500,
            body: `Error: ${error}`
        };
    }
    /*
    async function showfiles() {
        try {
          //let files = await readdir('/tmp');
          //for (const file of files)
          //  console.log('/tmp: ', file);
          let files = await readdir(process.env.efspath);
          for (const file of files)
            console.log('efs: ', file);
        } catch (err) {
          console.error(err);
        } 
    }
 */
    function execPromise(command) {
        console.log('execPromise', command);
        return new Promise((resolve, reject) => {
            let chproc = exec(command, (error, stdout, stderr) => {
                if (error) {
                    console.log('exec error', error, 'stderr', stderr, 'stdout', stdout);
                    resolve(`${stdout} stderr: ${stderr}`);
                } else {
                    //console.log('exec stdout', stdout);
                    resolve(stdout);
                }
            });
        });
    }

    async function downloadFileFromS3(bucket, key, downloadPath) {
        console.log('downloading file', bucket, key, downloadPath);
        const obj = await s3Client.send(
            new GetObjectCommand({ Bucket: bucket, Key: key }),
        );
        var ba = await obj.Body.transformToByteArray();
        console.log('downloaded file', ba.length);
        writeFileSync(
            downloadPath,
            ba
        );

    }

    async function uploadFileToS3(bucket, key, uploadPath) {
      try {
        console.log('uploading file', bucket, key, uploadPath);
        const fileContent = readFileSync(uploadPath);

            const params = {
                Bucket: bucket,
                Key: key,
                Body: fileContent,
                ACL: 'public-read'
            };

            await s3Client.send(
                new PutObjectCommand(params));
            } catch (error) {
                console.log('error uploading file', error);
            }
    }
                

}
