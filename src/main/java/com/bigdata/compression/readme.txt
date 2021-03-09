HDFS 文件压缩
hadoop archive -archiveName 最终生成的文件名.har -p 需要归档的文件或文件夹，可多写 最终生成的文件路径
例如：hadoop archive -archiveName rsync_2018.har -p /user/hdfs/rsync aaa/2018* bbb/2018* /archive/rsync