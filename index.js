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
    console.log("hello!! processing new s3 file!", key);
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
        const inputPath = `${process.env.EFS_PATH}/${inputFile}`;
        await downloadFileFromS3(json.inputBucket, inputKey, inputPath);
      
        console.log(`${inputPath} downloaded`,  existsSync(inputPath));

        const outputFile = `${id}_${basename(json.outputKey)}`; 

        let outputFilePath = `${process.env.EFS_PATH}/${outputFile}`;
        if  (existsSync(outputFilePath)) {
          unlinkSync(outputFilePath);
        }
        //let command = `/opt/bin/ffmpeg -i "${json.s3signedurl}"  -acodec libmp3lame -q:a 2 -metadata title="my title" -f mpegts ${outputFilePath}`;        //pipe:`;  
        
        let metadata = '';
        let coverfile = '';
        if (json.tags)
        {
            let tags = json.tags;
            if (tags.cover) {
                try {
                //convert the webp to a 200x200 jpg
                coverfile = `${process.env.EFS_PATH}/${id}_cover.jpg`; 
                let command = `/opt/bin/ffmpeg -i ${tags.cover} -loglevel error -vf scale=200:-1  -update true -vframes 1 ${coverfile}`; 
                await execPromise(command, 5);
                if (existsSync(coverfile))
                    metadata = `${metadata} -i ${coverfile} -map 0:a -map 1:0 -c:1 copy -id3v2_version 3`;
                } catch (err)
                {
                    console.log('cover error', err);
                    coverfile = '';
                }
            }
            if (tags.title) metadata = `${metadata} -metadata title="${tags.title}"`;
            if (tags.artist) metadata = `${metadata} -metadata artist="${tags.artist}"`;
            if (tags.album)  metadata = `${metadata} -metadata album="${tags.album}"`;
        }
        let command = `/opt/bin/ffmpeg -i ${inputPath} ${metadata} -loglevel error -acodec libmp3lame -q:a 2 -f mp3 ${outputFilePath}`; 
        await execPromise(command, 30);

   
        // Step 3: Upload the processed file back to S3
        await uploadFileToS3(json.outputBucket, json.outputKey,
          outputFilePath);
        
          unlinkSync(inputPath);
          unlinkSync(outputFilePath);
          if (coverfile) unlinkSync(coverfile);
        
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
    function execPromise(command, timeoutsecs) {
        console.log('execPromise', command, timeoutsecs);
        return new Promise((resolve, reject) => {
            try {
                //this timeout doesn't work
                let child = exec(command, { timeout: timeoutsecs*1000 }, (error, stdout, stderr) => {
                    clearTimeout(timeout);
                    if (error) {
                        console.log('exec error', error, 'stderr', stderr, 'stdout', stdout);
                        resolve(`${stdout} stderr: ${stderr}`);
                    } else {
                        resolve(stdout);
                    }
                });
                var timeout = setTimeout(() => {
                    console.log('Timeout');
                    try {
                      //this doesn't work either...will it ever get done?  Is it running forever?
                      process.kill(-child.pid, 'SIGKILL');
                    } catch (e) {
                      console.log('Cannot kill process');
                    }
                    resolve();
                  }, timeoutsecs*1000);
            } catch (err) {
                reject(`err: ${err}`)
            }
        });
    }

    async function downloadFileFromS3(bucket, key, downloadPath) {
        console.log('downloading file', bucket, key, downloadPath);
        const obj = await s3Client.send(
            new GetObjectCommand({ Bucket: bucket, Key: key }),
        );
        var ba = await obj.Body.transformToByteArray();
        try {
        writeFileSync(
            downloadPath,
            ba
        );
    } catch(e) {console.log('writeFileSync error', e);}

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
