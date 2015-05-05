hadoop.version=2.7.0
htsjdk.version=1.130
hadoop.jars=hadoop-${hadoop.version}/share/hadoop/common/hadoop-common-${hadoop.version}.jar:hadoop-${hadoop.version}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-${hadoop.version}.jar:hadoop-${hadoop.version}/share/hadoop/common/lib/hadoop-annotations-${hadoop.version}.jar:hadoop-${hadoop.version}/share/hadoop/common/lib/log4j-1.2.17.jar
hadoop.exe=hadoop-${hadoop.version}/bin/hadoop
htsjdk.jars=htsjdk-${htsjdk.version}/dist/commons-logging-1.1.1.jar:htsjdk-${htsjdk.version}/dist/htsjdk-${htsjdk.version}.jar:htsjdk-${htsjdk.version}/dist/commons-jexl-2.1.1.jar:htsjdk-${htsjdk.version}/dist/snappy-java-1.0.3-rc3.jar
comma := ,

.PHONY:all tests test1


all: dist/test01.jar  input/CEU.exon.2010_09.genotypes.vcf
	rm -rf output
	HADOOP_CLASSPATH=${htsjdk.jars} ${hadoop.exe} jar $< com.github.lindenb.hadoop.Test \
	 	input/CEU.exon.2010_09.genotypes.vcf output
	 cat output/*

htsjdk-${htsjdk.version}/dist/htsjdk-${htsjdk.version}.jar :
	rm -rf htsjdk-${htsjdk.version}
	curl -L -o ${htsjdk.version}.tar.gz "https://github.com/samtools/htsjdk/archive/${htsjdk.version}.tar.gz"
	tar xvfz ${htsjdk.version}.tar.gz
	rm ${htsjdk.version}.tar.gz
	(cd htsjdk-${htsjdk.version} && ant )

input/CEU.exon.2010_09.genotypes.vcf :
	mkdir -p input
	curl -o $@.gz "ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/paper_data_sets/a_map_of_human_variation/exon/snps/CEU.exon.2010_09.genotypes.vcf.gz"
	gunzip -f $@.gz

dist/test01.jar :  src/main/java/com/github/lindenb/hadoop/Test.java ${hadoop.exe} htsjdk-${htsjdk.version}/dist/htsjdk-${htsjdk.version}.jar
	mkdir -p tmp dist
	javac -d tmp -cp ${hadoop.jars}:${htsjdk.jars} -sourcepath src/main/java $< 
	jar cvf $@ -C tmp .
	rm -rf tmp

tests: test2 test1

test1: hadoop-${hadoop.version}/bin/hadoop
	rm -rf output
	mkdir -p input
	cp hadoop-${hadoop.version}/etc/hadoop/*.xml input
	${hadoop.exe}  jar hadoop-${hadoop.version}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${hadoop.version}.jar grep input output 'dfs[a-z.]+'
	cat output/*

${hadoop.exe} :
	rm -rf hadoop-${hadoop.version}
	curl -L -o hadoop-${hadoop.version}.tar.gz "http://apache.spinellicreations.com/hadoop/common/hadoop-${hadoop.version}/hadoop-${hadoop.version}.tar.gz"
	tar xvfz hadoop-${hadoop.version}.tar.gz
	rm hadoop-${hadoop.version}.tar.gz
	touch -c $@

