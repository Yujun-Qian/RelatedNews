package com.caishi.capricorn.algorithm.cluster;

import com.caishi.capricorn.common.base.*;

import static com.caishi.capricorn.common.base.FeedMessage.FEED_SOURCE_META_PRIORITY;

import com.caishi.capricorn.common.kafka.consumer.processor.JavaMsgProcessor;
import com.caishi.capricorn.common.kafka.consumer.ConsumerContainer;
import com.caishi.capricorn.common.kafka.producer.QueuedProducer;
import com.caishi.capricorn.common.kafka.consumer.processor.MsgProcessor;
import com.caishi.capricorn.common.kafka.consumer.processor.MsgProcessorInfo;
import com.caishi.capricorn.common.kafka.consumer.processor.StringMsgProcessor;

import static com.caishi.capricorn.common.base.FeedConstants.FEED_SOURCE_META_MESSAGE_STATUS;
import static com.caishi.capricorn.common.kafka.constants.KafkaConfigKey.ZK_SESSION;
import static com.caishi.capricorn.common.kafka.constants.KafkaConfigKey.ZK_SYNC;
import static com.caishi.capricorn.common.kafka.constants.KafkaConfigKey.COMMIT_TIME;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Date;
import java.util.Properties;
import java.util.Collection;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.Reader;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.bson.Document;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.MongoInputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Cluster {
    private String content;
    private BigInteger intSimHash;
    private String strSimHash;
    private Set<String> stopWords;
    private List<String> words;
    private Map<String, Integer> wordsMap;
    private int hashbits = 64;
    private boolean debug = false;
    private final static String[] strDigits = {"0", "1", "2", "3", "4", "5",
            "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};

    private static String graphite_host = "10.2.1.142";
    private static int graphite_port = 2003;
    private static String graphite_prefix = "carbon.simhash.prod";

    /**
     * @param content    newsContent
     */
    public Cluster(Set<String> stopWordsSet, String content) {
        this.stopWords = stopWordsSet;
        this.content = content;
        this.words = new ArrayList<String>();
        this.wordsMap = new HashMap<String, Integer>();
    }

    public Cluster(Set<String> stopWordsSet, String content, boolean debug) {
        this.stopWords = stopWordsSet;
        this.content = content;
        this.words = new ArrayList<String>();
        this.wordsMap = new HashMap<String, Integer>();
        this.debug = debug;
    }

    public Cluster(String content, int hashbits) {
        this.content = content;
        this.hashbits = hashbits;
    }

    public BigInteger getIntSimHash() {
        return intSimHash;
    }

    public void setIntSimHash(BigInteger intSimHash) {
        this.intSimHash = intSimHash;
    }


    public String getStrSimHash() {
        return strSimHash;
    }

    public void setStrSimHash(String strSimHash) {
        this.strSimHash = strSimHash;
    }

    public Map<String, Integer> getWordsMap() {
        return wordsMap;
    }

    /**
     * 生成特征词的的hash值
     *
     * @return
     */
    private BigInteger hash(String keywords) {
        if (keywords == null || keywords.length() == 0) {
            return new BigInteger("0");
        } else {
            char[] sourceArray = keywords.toCharArray();
            BigInteger x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
            BigInteger m = new BigInteger("1000003");
            BigInteger mask = new BigInteger("2").pow(this.hashbits).subtract(
                    new BigInteger("1"));
            for (char item : sourceArray) {
                BigInteger temp = BigInteger.valueOf((long) item);
                x = x.multiply(m).xor(temp).and(mask);
            }
            x = x.xor(new BigInteger(String.valueOf(keywords.length())));
            if (x.equals(new BigInteger("-1"))) {
                x = new BigInteger("-2");
            }
            return x;
        }
    }

    /**
     * 指纹压缩
     * 取两个二进制的异或，统计为1的个数，就是海明距离,确定两个文本的相似度，<3是近重复文本
     *
     * @return
     */

    public int hammingDistance(Cluster otherSimHash) {
        BigInteger x = this.intSimHash.xor(otherSimHash.intSimHash);
        int tot = 0;//x=0,海明距离为O;
        //统计x中二进制位数为1的个数
        while (x.signum() != 0) {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    public int hammingDistance(BigInteger simHash) {
        BigInteger x = this.intSimHash.xor(simHash);
        int tot = 0;//x=0,海明距离为O;
        //统计x中二进制位数为1的个数
        while (x.signum() != 0) {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    public static int hammingDistance(String simHash1, String simHash2) {
        BigInteger left = new BigInteger(simHash1, 16);
        BigInteger right = new BigInteger(simHash2, 16);
        BigInteger x = left.xor(right);
        int tot = 0;//x=0,海明距离为O;
        //统计x中二进制位数为1的个数
        while (x.signum() != 0) {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    /**
     * 获取索引列表
     * 如果海明距离取3，则分成四块，并得到每一块的bigInteger值 ，作为索引值使用
     *
     * @param simHash
     * @param distance
     * @return
     */
    public List<BigInteger> genSimHashBlock(Cluster simHash, int distance) {
        int eachBlockBitNum = this.hashbits / (distance + 1);
        List<BigInteger> simHashBlock = new ArrayList<BigInteger>();
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < this.intSimHash.bitLength(); i++) {
            boolean sr = simHash.intSimHash.testBit(i);
            if (sr) {
                buffer.append("1");
            } else {
                buffer.append("0");//补齐
            }
            if ((i + 1) % eachBlockBitNum == 0) {//够十六位时
                BigInteger eachValue = new BigInteger(buffer.toString(), 2);
                System.out.println("----" + eachValue);
                buffer.delete(0, buffer.length());
                simHashBlock.add(eachValue);
            }
        }
        return simHashBlock;
    }

    private static Set<String> initStopWordsSet() {
        Set<String> stopWords = new HashSet<String>();

        try {
            String encoding = "UTF-8";
            File file = new File("/home/software/stop_words.txt");
            if (file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    stopWords.add(line);
                }
                read.close();
            } else {
                System.out.println("File Not Found");
            }
        } catch (Exception e) {
            System.out.println("error reading stop words");
            e.printStackTrace();
        }

        return stopWords;
    }


    private static String readFile(String filePath) {
        String fileContent = null;
        try {
            String encoding = "UTF-8";
            File file = new File(filePath);
            if (file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                StringBuffer buffer = new StringBuffer();
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    buffer.append(line);
                }
                fileContent = buffer.toString();
                read.close();
            } else {
                System.out.println("File Not Found");
            }
        } catch (Exception e) {
            System.out.println("error reading stop words");
            e.printStackTrace();
        }

        return fileContent;
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Cluster");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final Set<String> stopWords = initStopWordsSet();
        Set<String> sensitiveWords = new HashSet<String>();

        String Queue_IPAddress1 = null;
        String Queue_IPAddress2 = null;
        String Queue_IPAddress3 = null;
        String Kafka_IPAddress1 = null;
        String Kafka_IPAddress2 = null;
        String Kafka_IPAddress3 = null;
        String SimhashDB_IPAddress = null;
        String NewsDB_IPAddress = null;
        InputStream is = Cluster.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = null;
        if (is != null) {
            try {
                prop = new Properties();
                prop.load(is);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        final String ENV = prop.getProperty("ENV");;
        int NewsDB_Port = 0;
        String DEBUG = "false";

        try {

            if (is != null) {
                Queue_IPAddress1 = prop.getProperty("Queue.IPAddress1");
                Queue_IPAddress2 = prop.getProperty("Queue.IPAddress2");
                Queue_IPAddress3 = prop.getProperty("Queue.IPAddress3");
                Kafka_IPAddress1 = prop.getProperty("Kafka.IPAddress1");
                Kafka_IPAddress2 = prop.getProperty("Kafka.IPAddress2");
                Kafka_IPAddress3 = prop.getProperty("Kafka.IPAddress3");
                SimhashDB_IPAddress = prop.getProperty("SimhashDB.IPAddress");
                NewsDB_IPAddress = prop.getProperty("NewsDB.IPAddress");
                System.out.println("newsDB port is : " + prop.getProperty("NewsDB.Port"));
                NewsDB_Port = Integer.parseInt(prop.getProperty("NewsDB.Port"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress(NewsDB_IPAddress, NewsDB_Port)));
        final MongoDatabase newsDB = mongoClient.getDatabase("news");
        final MongoCollection newsContent = newsDB.getCollection("newsContent");

        // Set configuration options for the MongoDB Hadoop Connector.
        Configuration mongodbConfig = new Configuration();
        // MongoInputFormat allows us to read from a live MongoDB instance.
        // We could also use BSONFileInputFormat to read BSON snapshots.
        mongodbConfig.set("mongo.job.input.format",
                "com.mongodb.hadoop.MongoInputFormat");
        mongodbConfig.set("mongo.input.fields",
                "{\"createtime\":1, \"debugInfo\":1, \"relatedNews\":1, \"categoryIds\":1, \"newsType\":1}");
        // mongo.input.query
        final Long timeStamp = new Date().getTime();
        Long timeSpan = 0L;
        if (ENV != null && ENV.equals("prod")) {
            timeSpan = 2592000L * 1000;
        } else {
            timeSpan = 48L * 60 * 60 * 1000;
        }
        final Long finalTimeSpan = timeSpan;
        Long startTime = timeStamp - finalTimeSpan;
        mongodbConfig.set("mongo.input.query",
                "{\"createtime\": {\"$gt\": " + startTime + "}}");

        // MongoDB connection string naming a collection to use.
        // If using BSON, use "mapred.input.dir" to configure the directory
        // where BSON files are located instead.
        mongodbConfig.set("mongo.input.uri",
                "mongodb://" + NewsDB_IPAddress + ":" + NewsDB_Port + "/news.newsContent");

        // Create an RDD backed by the MongoDB collection.
        JavaPairRDD<Object, BSONObject> documents = sc.newAPIHadoopRDD(
                mongodbConfig,            // Configuration
                MongoInputFormat.class,   // InputFormat: read from a live cluster.
                Object.class,             // Key class
                BSONObject.class          // Value class
        );

        System.out.println("we are here");

        Function<Tuple2<Object, BSONObject>, Boolean> filterGetLastQuar = new Function<Tuple2<Object, BSONObject>, Boolean>() {
            public Boolean call(Tuple2<Object, BSONObject> document) {
                Boolean result = false;
                //System.out.println(document._2);
                if (document._2.containsField("createtime")) {
                    Long createTime = (Long) document._2.get("createtime");
                    //System.out.println(createTime);
                    if ((timeStamp - createTime) > finalTimeSpan) {
                        return false;
                    }
                } else {
                    return false;
                }

                if (document._2.containsField("newsType")) {
                    String newsType = (String) document._2.get("newsType");

                    if (!newsType.equals("NEWS")) {
                        return false;
                    }
                } else {
                    return false;
                }

                if (document._2.containsField("debugInfo")) {
                    BSONObject debugInfo = (BSONObject) document._2.get("debugInfo");
                    if (debugInfo.containsField("tags")) {
                        result = true;
                        String tags = (String) debugInfo.get("tags");
                    }
                }
                return result;
            }
        };
        JavaPairRDD<Object, BSONObject> docsOfLastQuar = documents.filter(filterGetLastQuar);
        System.out.println("docsOfLastQuar : " + docsOfLastQuar.count());

        Function<Tuple2<Object, BSONObject>, Boolean> filterGetLastHour = new Function<Tuple2<Object, BSONObject>, Boolean>() {
            public Boolean call(Tuple2<Object, BSONObject> document) {
                Boolean result = false;
                //System.out.println(document._2);
                if (document._2.containsField("relatedNews")) {
                    return false;
                }

                if (document._2.containsField("createtime")) {
                    Long createTime = (Long) document._2.get("createtime");
                    //System.out.println(createTime);
                    if ((timeStamp - createTime) > 70 * 60 * 1000) {
                        return false;
                    }
                } else {
                    return false;
                }

                if (document._2.containsField("debugInfo")) {
                    BSONObject debugInfo = (BSONObject) document._2.get("debugInfo");
                    if (debugInfo.containsField("tags")) {
                        result = true;
                        String tags = (String) debugInfo.get("tags");
                    }
                }
                return result;
            }
        };
        JavaPairRDD<Object, BSONObject> docsOfLastHour = docsOfLastQuar.filter(filterGetLastHour);
        System.out.println("docsOfLastHour : " + docsOfLastHour.count());

        Function<BSONObject, Tuple2<JSONArray, BSONObject>> extractTags = new Function<BSONObject, Tuple2<JSONArray, BSONObject>>() {
            public Tuple2<JSONArray, BSONObject> call(BSONObject document) {
                JSONArray tag = null;
                //JSONArray category = null;
                BSONObject categoryIds = null;
                String newsType = null;
                BSONObject obj = new BasicBSONObject();

                try {
                    BSONObject debugInfo = (BSONObject) document.get("debugInfo");
                    String tags = (String) debugInfo.get("tags");
                    System.out.println("tags is: " + tags);
                    tag = (JSONArray) JSON.parseArray(tags);

                    categoryIds = (BSONObject) document.get("categoryIds");
                    System.out.println("categoryIds is: " + categoryIds);
                    newsType = (String) document.get("newsType");
                    System.out.println("newsType is: " + newsType);

                    obj.put("categoryIds", categoryIds);
                    obj.put("newsType", newsType);
                    //category = (JSONArray) JSON.parseArray(categoryIds.toString());
                } catch (Exception e) {
                }

                return new Tuple2<JSONArray, BSONObject>(tag, obj);
            }
        };
        JavaPairRDD<Object, Tuple2<JSONArray, BSONObject>> tagOfDocsLastHour = docsOfLastHour.mapValues(extractTags);
        JavaPairRDD<Object, Tuple2<JSONArray, BSONObject>> tagOfDocsLastQuar = docsOfLastQuar.mapValues(extractTags);
        final Map<Object, Tuple2<JSONArray, BSONObject>> tagMapLastHour = tagOfDocsLastHour.collectAsMap();

        PairFlatMapFunction<Tuple2<Object, Tuple2<JSONArray, BSONObject>>, Double, Tuple2<Tuple2<Object, Object>, BSONObject>> map =
                new PairFlatMapFunction<Tuple2<Object, Tuple2<JSONArray, BSONObject>>, Double, Tuple2<Tuple2<Object, Object>, BSONObject>>() {
                    public Iterable<Tuple2<Double, Tuple2<Tuple2<Object, Object>, BSONObject>>> call(Tuple2<Object, Tuple2<JSONArray, BSONObject>> pair) {
                        List<Tuple2<Double, Tuple2<Tuple2<Object, Object>, BSONObject>>> result = new ArrayList<Tuple2<Double, Tuple2<Tuple2<Object, Object>, BSONObject>>>();
                        BSONObject categoryIds = (BSONObject) pair._2._2;

                        for (Map.Entry<Object, Tuple2<JSONArray, BSONObject>> tag : tagMapLastHour.entrySet()) {
                            Double score = 0.0;
                            Double commonTag = 0.0;
                            Object obj1 = tag.getKey();
                            Object obj2 = pair._1;
                            Tuple2<Object, Object> item = new Tuple2<Object, Object>(obj1, obj2);

                            Set<String> strings = new HashSet<String>();
                            System.out.println(pair._2._1);
                            System.out.println(tag.getValue()._1);
                            JSONArray a = pair._2._1;
                            for (int i = 0; i < a.size(); i++) {
                                JSONObject tagObj = (JSONObject) a.get(i);
                                for (Map.Entry<String, Object> entry : tagObj.entrySet()) {
                                    String tagString = (String) entry.getKey();
                                    strings.add(tagString);
                                }
                            }
                            JSONArray b = tag.getValue()._1;
                            for (int i = 0; i < b.size(); i++) {
                                JSONObject tagObj = (JSONObject) b.get(i);
                                for (Map.Entry<String, Object> entry : tagObj.entrySet()) {
                                    String tagString = (String) entry.getKey();
                                    if (strings.contains(tagString)) {
                                        commonTag += 1.0;
                                    } else {
                                        strings.add(tagString);
                                    }
                                }
                            }
                            score = 100.0 * commonTag / strings.size();
                            System.out.println("score is: " + score);
                            if (score > 10E-6 && score < 100.0) {
                                Tuple2<Tuple2<Object, Object>, BSONObject> itemWithCategoryIds = new Tuple2<Tuple2<Object, Object>, BSONObject>(item, categoryIds);
                                result.add(new Tuple2(score, itemWithCategoryIds));
                            }
                        }

                        return result;
                    }
                };
        JavaPairRDD<Double, Tuple2<Tuple2<Object, Object>, BSONObject>> result = tagOfDocsLastQuar.flatMapToPair(map);

        JavaPairRDD<Double, Tuple2<Tuple2<Object, Object>, BSONObject>> result2 = result.sortByKey(false);
        System.out.println("result2.count is: " + result2.count());
        System.out.println("result2 is: ");
        //result2.saveAsTextFile("/directory/result0311_1");

        Map<String, Integer> newsCount = new HashMap<String, Integer>();
        List<Tuple2<Double, Tuple2<Tuple2<Object, Object>, BSONObject>>> out = result2.collect();
        final QueuedProducer queuedProducer = new QueuedProducer(Queue_IPAddress1 + ":9092," + Queue_IPAddress2 + ":9092," + Queue_IPAddress3 + ":9092");
        for (Tuple2<Double, Tuple2<Tuple2<Object, Object>, BSONObject>> entry : out) {
            String categoryIds = null;
            if (entry._2._2 != null) {
                if (entry._2._2.get("categoryIds") != null) {
                    categoryIds =entry._2._2.get("categoryIds").toString();
                }
            }
            System.out.println(entry._1 + " " + entry._2._1.toString() + " " + categoryIds);

            String newsToUpdate = null;
            if (entry._1 > 75.0) {
                String index1 = entry._2._1._1.toString();
                String index2 = entry._2._1._2.toString();
                int contentLength1 = 0;
                int contentLength2 = 0;

                if (index1.equals(index2)) {
                    continue;
                }

                /*
                FindIterable iterable = newsContent.find(new Document("_id", index1));
                MongoCursor cursor = iterable.iterator();
                while (cursor.hasNext()) {
                    Document document = (Document) cursor.next();
                    String content = (String) document.get("content");
                    contentLength1 = content.length();
                }

                iterable = newsContent.find(new Document("_id", index2));
                cursor = iterable.iterator();
                while (cursor.hasNext()) {
                    Document document = (Document) cursor.next();
                    String content = (String) document.get("content");
                    contentLength2 = content.length();
                }

                cursor.close();

                if (contentLength1 < contentLength2) {
                    newsToUpdate = entry._2._1._1.toString();
                } else {
                    newsToUpdate = entry._2._1._2.toString();
                }
                */
            }

            if (entry._1 > 60.0 || entry._1 < 2.90) {
                continue;
            }

            newsToUpdate = entry._2._1._1.toString();
            String relatedNews = entry._2._1._2.toString();
            BSONObject category = (BSONObject) entry._2._2.get("categoryIds");
            String newsType = (String) entry._2._2.get("newsType");
            Integer count = newsCount.get(newsToUpdate);
            if (count == null) {
                newsCount.put(newsToUpdate, 1);
                Document doc = new Document("newsId", relatedNews);
                doc.put("categoryIds", category);
                doc.put("newsType", newsType);
                doc.put("score", entry._1);

                newsContent.updateOne(new Document("_id", newsToUpdate),
                        new Document("$push", new Document("relatedNews", doc)),
                        new UpdateOptions().upsert(true));

                FindIterable iterable = newsContent.find(new Document("_id", newsToUpdate));
                MongoCursor cursor = iterable.iterator();
                while (cursor.hasNext()) {
                    Document document = (Document) cursor.next();
                    String newsStr = document.toJson();
                    System.out.println("**************** newsStr is: " + newsStr);

                    RelatedNewsMsg msg = new RelatedNewsMsg();
                    msg.setNewsId(newsToUpdate);
                    List<RelatedNews> newsList = new ArrayList<RelatedNews>();

                    JSONObject newsObj = (JSONObject) JSON.parse(newsStr);
                    JSONArray newsArr = (JSONArray) newsObj.get("relatedNews");
                    for (int k = 0; k < newsArr.size(); k++) {
                        JSONObject news = (JSONObject) newsArr.get(k);
                        RelatedNews relatedNewsObj = new RelatedNews();

                        String newsId = (String) news.get("newsId");
                        relatedNewsObj.setNewsId(newsId);
                        newsType = (String) news.get("newsType");
                        relatedNewsObj.setNewsType(MessageType.getByName(newsType));
                        Double score = ((BigDecimal) news.get("score")).doubleValue();
                        relatedNewsObj.setScore(score);
                        JSONArray categoryIdsArr = (JSONArray) news.get("categoryIds");
                        if (categoryIdsArr != null) {
                            List<Integer> ids = new ArrayList<Integer>();
                            for (int m = 0; m < categoryIdsArr.size(); m++) {
                                ids.add((Integer)categoryIdsArr.get(m));
                            }
                            relatedNewsObj.setCategoryIds(ids);
                        }

                        newsList.add(relatedNewsObj);
                    }

                    msg.setRelatedNews(newsList);
                    queuedProducer.sendMessage("topic_news_related", msg);
                }
                cursor.close();

            } else if (count < 10) {
                newsCount.put(newsToUpdate, count + 1);

                Document doc = new Document("newsId", relatedNews);
                doc.put("categoryIds", category);
                doc.put("newsType", newsType);
                doc.put("score", entry._1);

                newsContent.updateOne(new Document("_id", newsToUpdate),
                        new Document("$push", new Document("relatedNews", doc)),
                        new UpdateOptions().upsert(true));


                FindIterable iterable = newsContent.find(new Document("_id", newsToUpdate));
                MongoCursor cursor = iterable.iterator();
                while (cursor.hasNext()) {
                    Document document = (Document) cursor.next();
                    String newsStr = document.toJson();
                    System.out.println("**************** newsStr is: " + newsStr);

                    RelatedNewsMsg msg = new RelatedNewsMsg();
                    msg.setNewsId(newsToUpdate);
                    List<RelatedNews> newsList = new ArrayList<RelatedNews>();

                    JSONObject newsObj = (JSONObject) JSON.parse(newsStr);
                    JSONArray newsArr = (JSONArray) newsObj.get("relatedNews");
                    for (int k = 0; k < newsArr.size(); k++) {
                        JSONObject news = (JSONObject) newsArr.get(k);
                        RelatedNews relatedNewsObj = new RelatedNews();

                        String newsId = (String) news.get("newsId");
                        relatedNewsObj.setNewsId(newsId);
                        newsType = (String) news.get("newsType");
                        relatedNewsObj.setNewsType(MessageType.getByName(newsType));
                        Double score = ((BigDecimal) news.get("score")).doubleValue();
                        relatedNewsObj.setScore(score);
                        JSONArray categoryIdsArr = (JSONArray) news.get("categoryIds");
                        if (categoryIdsArr != null) {
                            List<Integer> ids = new ArrayList<Integer>();
                            for (int m = 0; m < categoryIdsArr.size(); m++) {
                                ids.add((Integer)categoryIdsArr.get(m));
                            }
                            relatedNewsObj.setCategoryIds(ids);
                        }

                        newsList.add(relatedNewsObj);
                    }

                    msg.setRelatedNews(newsList);
                    queuedProducer.sendMessage("topic_news_related", msg);
                }
                cursor.close();
            }
        }

        /*
        {
            Document doc = new Document("sentence", combinedSentence);
            doc.put("timeStamp", timeStamp);
            doc.put("newsId", id);
            doc.put("srcLink", srcLink);
            sentenceCollection.updateOne(new Document("index", sentenceId),
                    new Document("$push", new Document("sentences", doc)),
                    new UpdateOptions().upsert(true));
        }
        {
            updated = true;
            Document docToUpdate = new Document("index", sentenceId);
            docToUpdate.put("sentences.sentence", sentenceStr);
            Document doc = new Document("sentences.$.timeStamp", timeStamp);
            sentenceCollection.updateOne(docToUpdate, new Document("$set", doc));
            doc = new Document("sentences.$.newsId", id);
            sentenceCollection.updateOne(docToUpdate, new Document("$set", doc));
            doc = new Document("sentences.$.srcLink", srcLink);
            sentenceCollection.updateOne(docToUpdate, new Document("$set", doc));
        }
        */

        // Create a separate Configuration for saving data back to MongoDB.
        /*
        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri",
                "mongodb://10.1.1.122:27017/caishi.testagain");
        */

        sc.stop();

        // Save this RDD as a Hadoop "file".
        // The path argument is unused; all documents will go to 'mongo.output.uri'.
        /*
        documents.saveAsNewAPIHadoopFile(
                "file:///this-is-completely-unused",
                Object.class,
                BSONObject.class,
                MongoOutputFormat.class,
                outputConfig
        );
        */
    }
}
