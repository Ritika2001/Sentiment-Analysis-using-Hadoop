--Register all jar files
REGISTER /home/ritika/Hadoop/pig-0.12.0-cdh5.3.2/contrib/piggybank/java/piggybank.jar
REGISTER hdfs://localhost:9000/NLP/Libraries/datafu-pig-incubating-1.3.0.jar --UDF for POSTagger
REGISTER hdfs://localhost:9000/NLP/Libraries/datafu-1.2.0.jar
define VAR datafu.pig.stats.VAR();
REGISTER hdfs://localhost:9000/NLP/Libraries/User-Defines-Functions/Separate.jar;
REGISTER hdfs://localhost:9000/NLP/Libraries/User-Defines-Functions/PreProcess.jar;
REGISTER hdfs://localhost:9000/NLP/Libraries/User-Defines-Functions/RATE.jar
REGISTER hdfs://localhost:9000/NLP/Libraries/User-Defines-Functions/cdf.jar
REGISTER hdfs://localhost:9000/NLP/Libraries/User-Defines-Functions/hmean.jar

--Load all the datasets
load_training_set = load '/NLP/Datasets/Training_Dataset.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','UNIX','SKIP_INPUT_HEADER') as (rating:chararray, id:chararray, datetime:chararray, query:chararray, name:chararray, text:chararray); 
load_test_set = load '/NLP/Datasets/Test_Dataset.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','UNIX','SKIP_INPUT_HEADER') as (rating:chararray, id:chararray, datetime:chararray, query:chararray, name:chararray, text:chararray); 
dict = load '/NLP/Datasets/testdict.txt' as dictwords:chararray;

--Preprocessing the datasets
extract = foreach load_training_set generate rating, text;
a = foreach extract generate rating, FLATTEN(TOKENIZE(text)) as word; --tokenize the tweets
b = filter a by NOT(word matches '.*@.*'); --remove domain names
c = filter b by NOT(word matches '.*http.*'); --remove hyperlinks
d = foreach c generate rating, REPLACE(word, '#', '') as word; --remove # from hashtags
e = foreach d generate rating, REPLACE(word, '/', ' ') as word; --separate words
f = foreach e generate rating, REPLACE(word, '&quot;', '') as word; --remove unwanted characters
g = filter f by NOT(word matches '.*&amp.*;' or word matches '.*&lt;.*');
h = foreach g generate rating, REPLACE(word,'([^a-zA-Z\\s]+)', ' ') as word;
i = foreach h generate rating, FLATTEN(TOKENIZE(word)) as word;
j = foreach i generate rating, LOWER(word) as word; --convert all words to lower case
m = join j by word left outer, dict by dictwords using 'replicated'; 
n1 = filter m by $2 is not null;
n3 = foreach n1 generate $0 as rating, $1 as word;
n2 = filter m by $2 is null;
y = foreach n2 generate $0, $1, SeparateStringWords($1) as wordcorrect;
x1 = filter y by $2 is not null;
x3 = foreach x1 generate $0 as rating, FLATTEN(TOKENIZE($2)) as word;
x2 = filter y by $2 is null;
z = foreach x2 generate $0, $1, PreProcess($1) as wordcorrect;
z1 = foreach z generate $0 as rating, $2 as word;
final = union n3, x3, z1;
finalset = foreach final generate $0 as rating, $1 as word;

--Metric values for each word in the dataset
SPLIT finalset into k1 if rating== '0', k2 if rating =='4';
grp1 = group k1 by word;
cnt1 = foreach grp1 generate group, COUNT($1) as freq;
grp2 = group k2 by word;
cnt2 = foreach grp2 generate group, COUNT($1) as freq;
grp3 = group finalset by word;
cnt3 = foreach grp3 generate group, COUNT($1) as freq;
jn = JOIN cnt1 by group, cnt2 by group, cnt3 by group;
jn2 = order jn by $5 DESC;
final = foreach jn2 generate $4 as word, $1 as negative, $3 as positive, $5 as total;
x = foreach final generate word, negative,Rate(negative,total) as neg_rate, positive,Rate(positive,total) as pos_rate, total;
y = foreach x generate $0,$1,$2, (float)negative/10032001 as neg_pct, $3, $4, (float)positive/9077635 as pos_pct; 
yn = foreach y generate word, $1 as neg, neg_rate, neg_pct;
yp = foreach y generate word, $4 as pos, pos_rate, pos_pct;

yn1 = foreach (GROUP yn all) generate AVG(yn.neg_rate) as neg_rate_avg, AVG(yn.neg_pct) as neg_pct_avg, SQRT(VAR(yn.neg_rate)) as neg_rate_sd, SQRT(VAR(yn.neg_pct)) as neg_pct_sd;
yn2 = CROSS yn,yn1;

yn3 = foreach yn2 generate $0 as word, $1 as neg, $2 as neg_rate, $3 as neg_pct, cdf($2, $4, $6) as neg_rate_cdf, cdf($3, $5, $7) as neg_pct_cdf;
ynfinal = foreach yn3 generate $0, $1,$2,$3,$4,$5, hmean($4,$5) as hmean;

yp1 = foreach (GROUP yp all) generate AVG(yp.pos_rate) as pos_rate_avg, AVG(yp.pos_pct) as pos_pct_avg, SQRT(VAR(yp.pos_rate)) as pos_rate_sd, SQRT(VAR(yp.pos_pct)) as pos_pct_sd;
yp2 = CROSS yp,yp1;
yp3 = foreach yp2 generate $0 as word, $1 as pos, $2 as pos_rate, $3 as pos_pct, cdf($2, $4, $6) as pos_rate_cdf, cdf($3, $5, $7) as pos_pct_cdf;
ypfinal = foreach yp3 generate $0, $1,$2,$3,$4,$5, hmean($4,$5) as hmean;

finale = JOIN ypfinal by word full outer, ynfinal by word;
xee = foreach finale generate $0 as posword, $6 as posfreq, $7 as negword, $13 as negfreq;

--Store the generated confusion matrix
STORE extract INTO '/NLP/output' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE','UNIX','WRITE_OUTPUT_HEADER');
