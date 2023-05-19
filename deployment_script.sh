#!/bin/zsh

rm -f -r ./package
rm -f deployment-package.zip
pip3.9 install --target ./package -r requirements.txt
pip3.9 install \
    --platform manylinux2014_x86_64 \
    --target=./package \
    --implementation cp \
    --python 3.9 \
    --only-binary=:all: --upgrade \
    'psycopg[binary]'

cp lambda_function.py package/.
cp -R helpers package/.

cd package
zip -r ../deployment-package.zip .
cd ..