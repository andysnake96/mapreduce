#!/usr/bin/env bash
#UPDATE SCRIPTS AND CONFIGURATION
./start.sh upload
cd ../configurations/
./updateRemoteConfigFile.sh
cd ..
#UPDATE CODE IN EXTRA COMPRESSES ZIP IN S3 PRJ BUCKET
rm tmpDump.zip
zip  -9 tmpDump.zip -x '.git/*' -x '.idea/*' -x 'src/*' -x 'pkg/*' -x 'out/*' -x 'txtSrc/*' -x 'test/*' -r .
aws s3 cp tmpDump.zip s3://mapreducechunks/
