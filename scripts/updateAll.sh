#!/usr/bin/env bash
#UPDATE SCRIPTS AND CONFIGURATION
./start.sh upload
cd ../configurations/
./updateRemoteConfigFile.sh
cd ..
rm -f master/master worker/worker #clean builds before upload
#UPDATE CODE IN EXTRA COMPRESSES ZIP IN S3 PRJ BUCKET
rm tmpDump.zip
zip  -9 tmpDump.zip -x '.git/*' -x '.idea/*' -x 'src/*' -x 'pkg/*' -x '*log' -x 'out/*'  -x 'test/*' -r .
aws s3 cp tmpDump.zip s3://mapreducechunks/
