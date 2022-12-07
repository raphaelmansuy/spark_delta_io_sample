#!/usr/bin/env bash 


echo "Size in MB of each file in the directory ./users.delta recursively"
if [ -d "./users.delta" ]; then
	du -ah ./users.delta/**/*.parquet 
fi

echo "Size in MB of each file in the directory ./users.parquet recursively"
if [ -d "./users.parquet" ]; then
	du -ah ./users.parquet/**/*.parquet 
fi




