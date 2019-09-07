package aws_SDK_wrap

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"os"
)

const (
	S3_REGION = "eu-central-1"
	S3_BUCKET = "mapreducechunks"
)

func InitS3Links(region string) (*s3manager.Downloader, *s3manager.Uploader) {
	// create 2 s3 session for S3 downloader and uploader
	// Create a single AWS session
	sessionS3, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Fatal(err)
	}
	//// create downloader and uploader on a newly created s3 session
	downloader := s3manager.NewDownloader(sessionS3)
	// Create a single AWS session
	sessionS3, err = session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Fatal(err)
	}
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sessionS3)
	return downloader, uploader
}

/*func main(){
	downloader,uploader:=initS3Links(S3_REGION)
	key:="ASDF"
	data:="FDSA"
	_ = UploadDATA(uploader, data, key,S3_BUCKET)
	buf := make([]byte, len(data))
	_= DownloadDATA(downloader,S3_BUCKET,key,buf)
}*/
//  will  be set file info like content type and encryption on the uploaded file.
func UploadDATA(uploader *s3manager.Uploader, dataStr string, strKey string, bucket string) error {

	// Upload input parameters
	upParams := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(strKey),
		ACL:    aws.String("private"),
		Body:   bytes.NewReader([]byte(dataStr)),
	}

	// Perform upload with options different than the those in the Uploader.
	result, err := uploader.Upload(upParams)
	if err != nil {
		_, _ = fmt.Fprint(os.Stderr, err, "<-->", result)
		return err
	}

	return nil
}

func DownloadDATA(downloader *s3manager.Downloader, bucket, key string, bufOut []byte, sizeCheck bool) error {
	//download data from s3 and put it i bufOut, byte buffer pre allocated with expected size, propagated eventual errors
	writer := aws.NewWriteAtBuffer(bufOut)
	s3InputOption := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	downloadedSize, err := downloader.Download(writer, s3InputOption)
	if err != nil {
		_, _ = fmt.Fprint(os.Stderr, err)
	}

	if sizeCheck && (int64(len(bufOut)) != downloadedSize) {
		return errors.New("download size mismatch with expected!\n")
	}
	println("downloaded ", key, " of size ", downloadedSize)
	return nil
}
