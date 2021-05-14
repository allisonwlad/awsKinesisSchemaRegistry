package br.com.awsExamples.aws;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

public class AwsKinesisClient {

    public static final String AWS_ACCESS_KEY = "aws.accessKeyId";
    public static final String AWS_SECRET_KEY = "aws.secretKey";

    static {
        System.setProperty(AWS_ACCESS_KEY, "AKIAXPBYIUZU5IXLRBO3");
        System.setProperty(AWS_SECRET_KEY, "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");
    }

    public static AmazonKinesis getKinesisClient(){
        return AmazonKinesisClientBuilder
                .standard()
                .withRegion(Regions.SA_EAST_1)
                .build();
    }
}
