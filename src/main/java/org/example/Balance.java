package org.example;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;

public class Balance {

    // Database connection strings
    private static final String MONGO_URL = "mongodb://PGS:PGSReady@172.31.88.8:27127/PGS?authSource=PGS&autoIndexCreation=true";

    public static void main(String[] args)  {

        MongoClient mongoClient = null;
        // Connect to MongoDB
        System.out.println("Connecting to MongoDB...");
        mongoClient = MongoClients.create(MONGO_URL);
//        MongoClientURI mongoUri = new MongoClientURI(MONGO_URL);
//        mongoClient = new MongoClient(mongoUri);
//        MongoDatabase mongoDatabase = mongoClient.getDatabase("PGS");
        MongoDatabase mongoDatabase = mongoClient.getDatabase("PGS");
        MongoCollection<Document> collection = mongoDatabase.getCollection("userRoleMaster");
        System.out.println("✓ Connected to MongoDB successfully\n");
        // Create MongoTemplate and inject it
        MongoTemplate mongoTemplate = new MongoTemplate(
                new SimpleMongoClientDatabaseFactory(mongoClient, "PGS")
        );
        System.out.println("===========================================");
        System.out.println("Get balance from purchase and payout MongoDB collection");
        System.out.println("===========================================\n");
        Payout payout = new Payout("payout-UPI", "151", "MXN", "PO123");
        Charges charges = new Charges(0.3f, 0.0f, 3.5f, 20.0f, 40.0f, 10.0f); // example values
        List<MDR> mdrList = Collections.singletonList(new MDR(1.0f));
        PurchaseDAL purchaseDAL = new PurchaseDAL(mongoTemplate);

        long startTime = System.nanoTime();
        ApiError result = getBalanceNew(payout, purchaseDAL, charges, mdrList);
        long endTime = System.nanoTime();

        System.out.println("Result: " + result);
        System.out.println("Execution time: " + (endTime - startTime)/1_000_000 + " ms");
    }

    public static ApiError getBalanceNew(Payout PR, PurchaseDAL purchaseDAL, Charges ch, List<MDR> mdr)
            {
        ApiError AE = new ApiError();
        String payoutMethod = PR.getPayoutMethod();
        String paymentMethod = null;
        float totalTxnFee = 0, mdrf = 0, rrp = 0, rf = 0, cbf = 0, balance = 0;

        if (payoutMethod != null && payoutMethod.toLowerCase().startsWith("payout-")) {
            paymentMethod = payoutMethod.substring(7).toUpperCase();
        }

        AggregateTransactionsResult totalPaidResult = purchaseDAL.getTotalPaidAmount(PR.getMerchant_id(), paymentMethod, PR.getCurrency());

        if (ch != null) {
            totalTxnFee += ch.getTran_fee() * totalPaidResult.getGeneralStatusCount();
            totalTxnFee += ch.getDecline_fee() * totalPaidResult.getErrorCount();
            rrp += (totalPaidResult.getTotalSum() * ch.getRolling_res_per()) / 100;
        }

        if (mdr != null && !mdr.isEmpty()) {
            mdrf += (totalPaidResult.getTotalSum() * mdr.get(0).getRate()) / 100;
        }

        AggregateTransactionsResult totalPayoutResult = purchaseDAL.getTotalPayoutAmount(PR.getMerchant_id(), payoutMethod, PR.getCurrency());

        if (ch != null) {
            totalTxnFee += ch.getTran_fee() * totalPayoutResult.getTransactionCount();
            totalTxnFee += ch.getDecline_fee() * totalPayoutResult.getErrorCount();
        }

        if (mdr != null && !mdr.isEmpty()) {
            mdrf += (totalPayoutResult.getTotalSum() * mdr.get(0).getRate()) / 100;
        }

        AggregateTransactionsResult refundedOrChargebackResult = purchaseDAL.getRefundedOrChargebackAmount(PR.getMerchant_id(), paymentMethod, PR.getCurrency());

        if (ch != null) {
            rf += ch.getRefund_fee() * refundedOrChargebackResult.getRefundCount();
            rf += ch.getFraudRefundFee() * refundedOrChargebackResult.getErrorCount();
            cbf += ch.getCharge_back_fee() * refundedOrChargebackResult.getChargebackCount();
        }

        balance = totalPaidResult.getTotalSum() - rrp - totalPayoutResult.getTotalSum() - totalTxnFee - mdrf
                - refundedOrChargebackResult.getTotalSum() - rf - cbf;

        AE.setCode("success");
        AE.setBalance(String.valueOf(balance));
        return AE;
    }

}

// Mock classes

class ApiError {
    private String code;
    private String balance;
    public void setCode(String code) { this.code = code; }
    public void setBalance(String balance) { this.balance = balance; }
    public String toString() { return "Code: " + code + ", Balance: " + balance; }
}

class Payout {
    private String payoutMethod;
    private String merchantId;
    private String currency;
    private String payoutId;

    public Payout(String method, String merchantId, String currency, String payoutId) {
        this.payoutMethod = method;
        this.merchantId = merchantId;
        this.currency = currency;
        this.payoutId = payoutId;
    }
    public String getPayoutMethod() { return payoutMethod; }
    public String getMerchant_id() { return merchantId; }
    public String getCurrency() { return currency; }
    public String getPayoutId() { return payoutId; }
}

class Charges {
    private float tran_fee, decline_fee, refund_fee, fraudRefundFee, charge_back_fee, rolling_res_per;
    public Charges(float t, float d, float r, float f, float cb, float rr) {
        this.tran_fee = t; this.decline_fee = d; this.refund_fee = r;
        this.fraudRefundFee = f; this.charge_back_fee = cb; this.rolling_res_per = rr;
    }
    public float getTran_fee() { return tran_fee; }
    public float getDecline_fee() { return decline_fee; }
    public float getRefund_fee() { return refund_fee; }
    public float getFraudRefundFee() { return fraudRefundFee; }
    public float getCharge_back_fee() { return charge_back_fee; }
    public float getRolling_res_per() { return rolling_res_per; }
}

class MDR {
    private float rate;
    public MDR(float rate) { this.rate = rate; }
    public float getRate() { return rate; }
}

class AggregateTransactionsResult {
    private float totalSum;
    private int transactionCount;
    private int generalStatusCount;
    private int errorCount;

    private int refundCount;
    private int chargebackCount;

    public AggregateTransactionsResult(float totalSum,int transactionCount) {
        this.totalSum = totalSum;
        this.transactionCount = transactionCount;
    }

    public AggregateTransactionsResult(float totalSum, int transactionCount,int generalStatusCount,int errorCount, int refundCount,
                                       int chargebackCount) {
        this.totalSum = totalSum;
        this.transactionCount = transactionCount;
        this.generalStatusCount = generalStatusCount;
        this.errorCount = errorCount;
        this.refundCount = refundCount;
        this.chargebackCount = chargebackCount;
    }

    public float getTotalSum() {
        return totalSum;
    }

    public void setTotalSum(float totalSum) {
        this.totalSum = totalSum;
    }

    public int getTransactionCount() {
        return transactionCount;
    }

    public void setTransactionCount(int transactionCount) {
        this.transactionCount = transactionCount;
    }

    public int getRefundCount() {
        return refundCount;
    }

    public void setRefundCount(int refundCount) {
        this.refundCount = refundCount;
    }

    public int getChargebackCount() {
        return chargebackCount;
    }

    public void setChargebackCount(int chargebackCount) {
        this.chargebackCount = chargebackCount;
    }

    public int getGeneralStatusCount() {
        return generalStatusCount;
    }

    public void setGeneralStatusCount(int generalStatusCount) {
        this.generalStatusCount = generalStatusCount;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(int errorCount) {
        this.errorCount = errorCount;
    }
}

class PurchaseDAL {

    private MongoTemplate mongoTemplate;

    // Constructor injection
    public PurchaseDAL(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public AggregateTransactionsResult getTotalPaidAmount(String merchantId, String method, String currency) {
        List<AggregationOperation> operations = new ArrayList<>();

        // Build shared base criteria
        List<Criteria> baseCriteria = new ArrayList<>();
        baseCriteria.add(Criteria.where("purchase.currency").is(currency));
        baseCriteria.add(Criteria.where("isSettlementPending").ne(0));  //  Common across all

        ZoneId utcZone = ZoneOffset.UTC;

        LocalDate oneMonthAgo = LocalDate.now(utcZone).minusMonths(2);
        ZonedDateTime oneMonthAgoZDT = oneMonthAgo.atStartOfDay(utcZone);

        long oneMonthAgoEpoch = oneMonthAgoZDT.toEpochSecond();
        baseCriteria.add(Criteria.where("created_on").gte(oneMonthAgoEpoch));
        System.out.println("created_on >= " + oneMonthAgoEpoch);

        if (merchantId != null) {
            baseCriteria.add(Criteria.where("merchant_id").is(merchantId));
        }

//	    if (paymentMethod != null) {
//	        baseCriteria.add(new Criteria().orOperator(
//	            Criteria.where("paymentMethod").is(paymentMethod),
//	            Criteria.where("appliedPaymentMethod").is(paymentMethod)
//	        ));
//	    }

        operations.add(Aggregation.facet(
                                // --- 1. PAID Transactions ---
                                Aggregation.match(new Criteria().andOperator(
                                        Criteria.where("status").is("PAID"),
                                        new Criteria().andOperator(baseCriteria.toArray(new Criteria[0]))
                                )),
                                Aggregation.group()
                                        .sum("purchase.total").as("totalSum")
                                        .count().as("transactionCount")
                        ).as("paidResults")

                        // --- 2. General status count ---
                        .and(
                                Aggregation.match(new Criteria().andOperator(
                                        Criteria.where("status").in("CREATED", "EXPIRED", "PAID", "PAYMENT_IN_PROCESS"),
                                        new Criteria().andOperator(baseCriteria.toArray(new Criteria[0]))
                                )),
                                Aggregation.group().count().as("generalStatusCount")
                        ).as("generalStatusResults")

                        // --- 3. ERROR status count ---
                        .and(
                                Aggregation.match(new Criteria().andOperator(
                                        Criteria.where("status").is("ERROR"),
                                        new Criteria().andOperator(baseCriteria.toArray(new Criteria[0]))
                                )),
                                Aggregation.group().count().as("errorCount")
                        ).as("errorResults")
        );

        Aggregation aggregation = Aggregation.newAggregation(operations);
        System.out.println("getTotalPaidAmount : " + " "+aggregation.toString() );
        AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, "purchase", Document.class);
        System.out.println("getTotalPaidAmount : "  +  results );
        Document result = results.getUniqueMappedResult();

        float totalSum = 0.0f;
        int paidTxnCount = 0;
        int generalStatusCount = 0;
        int errorTxnCount = 0;

        if (result != null) {
            List<Document> paid = (List<Document>) result.get("paidResults");
            if (!paid.isEmpty()) {
                Document doc = paid.get(0);
                totalSum = doc.getDouble("totalSum") != null ? doc.getDouble("totalSum").floatValue() : 0.0f;
                paidTxnCount = doc.getInteger("transactionCount") != null ? doc.getInteger("transactionCount") : 0;
            }

            List<Document> general = (List<Document>) result.get("generalStatusResults");
            if (!general.isEmpty()) {
                Document doc = general.get(0);
                generalStatusCount = doc.getInteger("generalStatusCount") != null ? doc.getInteger("generalStatusCount") : 0;
            }

            List<Document> error = (List<Document>) result.get("errorResults");
            if (!error.isEmpty()) {
                Document doc = error.get(0);
                errorTxnCount = doc.getInteger("errorCount") != null ? doc.getInteger("errorCount") : 0;
            }
        }

        return new AggregateTransactionsResult(totalSum, paidTxnCount, generalStatusCount, errorTxnCount,0,0);
    }

    public AggregateTransactionsResult getTotalPayoutAmount(String merchantId, String method, String currency) {
        List<AggregationOperation> operations = new ArrayList<>();

        // Common base criteria
        List<Criteria> baseCriteria = new ArrayList<>();
        baseCriteria.add(Criteria.where("isSettlementPending").ne(0));
        baseCriteria.add(Criteria.where("currency").is(currency));
        ZoneId utcZone = ZoneOffset.UTC;

        LocalDate sixMonthsAgo = LocalDate.now(utcZone).minusMonths(6);
        ZonedDateTime sixMonthsAgoZDT = sixMonthsAgo.atStartOfDay(utcZone);

        long sixMonthsAgoEpoch = sixMonthsAgoZDT.toEpochSecond();
        baseCriteria.add(Criteria.where("created_on").gte(sixMonthsAgoEpoch));
        System.out.println("created_on >= " + sixMonthsAgoEpoch);

        if (merchantId != null) {
            baseCriteria.add(Criteria.where("merchant_id").is(merchantId));
        }

        // Uncomment if payoutMethod filtering is required
//	    if (payoutMethod != null) {
//	        baseCriteria.add(Criteria.where("payoutMethod").is(payoutMethod));
//	    }

        operations.add(Aggregation.facet(
                                // Pipeline 1: sum and count for PAID, PAYOUT_IN_PROCESS, DISPATCHED
                                Aggregation.match(new Criteria().andOperator(
                                        Criteria.where("status").in("PAID", "PAYOUT_IN_PROCESS", "DISPATCHED"),
                                        new Criteria().andOperator(baseCriteria.toArray(new Criteria[0]))
                                )),
                                Aggregation.group().sum("amount").as("totalSum").count().as("transactionCount")
                        ).as("payoutResults")

                        // Pipeline 2: count for ERROR
                        .and(
                                Aggregation.match(new Criteria().andOperator(
                                        Criteria.where("status").is("ERROR"),
                                        new Criteria().andOperator(baseCriteria.toArray(new Criteria[0]))
                                )),
                                Aggregation.group().count().as("errorCount")
                        ).as("errorResults")
        );

        Aggregation aggregation = Aggregation.newAggregation(operations);
        System.out.println("getTotalPayoutAmount : " + aggregation.toString());

        AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, "payout", Document.class);
        System.out.println("getTotalPayoutAmount : " + results);

        Document result = results.getUniqueMappedResult();

        float totalSum = 0.0f;
        int transactionCount = 0;
        int errorCount = 0;

        if (result != null) {
            List<Document> payoutList = (List<Document>) result.get("payoutResults");
            if (!payoutList.isEmpty()) {
                Document payoutData = payoutList.get(0);
                Double sum = payoutData.getDouble("totalSum");
                totalSum = sum != null ? sum.floatValue() : 0.0f;

                Integer count = payoutData.getInteger("transactionCount");
                transactionCount = count != null ? count : 0;
            }

            List<Document> errorList = (List<Document>) result.get("errorResults");
            if (!errorList.isEmpty()) {
                Integer count = errorList.get(0).getInteger("errorCount");
                errorCount = count != null ? count : 0;
            }
        }

        return new AggregateTransactionsResult(totalSum, transactionCount, 0,errorCount,0,0);
    }

    public AggregateTransactionsResult getRefundedOrChargebackAmount(String merchantId, String method, String currency) {
        List<AggregationOperation> operations = new ArrayList<>();

        // Common filters for paymentMethod and merchantId
        List<Criteria> commonFilters = new ArrayList<>();
        if (merchantId != null) {
            commonFilters.add(Criteria.where("merchant_id").is(merchantId));
        }
//	    if (paymentMethod != null) {
//	        commonFilters.add(
//	            new Criteria().orOperator(
//	                Criteria.where("paymentMethod").is(paymentMethod),
//	                Criteria.where("appliedPaymentMethod").is(paymentMethod)
//	            )
//	        );
//	    }
        commonFilters.add(Criteria.where("purchase.currency").is(currency));
        ZoneId utcZone = ZoneOffset.UTC;

        LocalDate sixMonthsAgo = LocalDate.now(utcZone).minusMonths(6);
        ZonedDateTime sixMonthsAgoZDT = sixMonthsAgo.atStartOfDay(utcZone);

        long sixMonthsAgoEpoch = sixMonthsAgoZDT.toEpochSecond();
        commonFilters.add(Criteria.where("created_on").gte(sixMonthsAgoEpoch));
        System.out.println("created_on >= " + sixMonthsAgoEpoch);

        // $facet to calculate 3 parallel pipelines
        operations.add(Aggregation.facet(

                                // Pipeline 1: totalSum for REFUNDED/CHARGEBACK and isSettlementPending ∈ [4, 9]
                                Aggregation.match(
                                        new Criteria().andOperator(
                                                Criteria.where("status").in("REFUNDED", "CHARGEBACK"),
                                                Criteria.where("isSettlementPending").in(4, 9),
                                                new Criteria().andOperator(commonFilters.toArray(new Criteria[0]))
                                        )
                                ),
                                Aggregation.group().sum("purchase.total").as("totalSum")
                        ).as("sumResults")

                        // Pipeline 2: refundedCount for REFUNDED with isSettlementPending != 0
                        .and(
                                Aggregation.match(
                                        new Criteria().andOperator(
                                                Criteria.where("status").is("REFUNDED"),
                                                Criteria.where("isSettlementPending").ne(0),
                                                new Criteria().andOperator(commonFilters.toArray(new Criteria[0]))
                                        )
                                ),
                                Aggregation.group().count().as("refundedCount")
                        ).as("refundedResults")

                        // Pipeline 3: chargebackCount for CHARGEBACK with isSettlementPending != 0
                        .and(
                                Aggregation.match(
                                        new Criteria().andOperator(
                                                Criteria.where("status").is("CHARGEBACK"),
                                                Criteria.where("isSettlementPending").ne(0),
                                                new Criteria().andOperator(commonFilters.toArray(new Criteria[0]))
                                        )
                                ),
                                Aggregation.group().count().as("chargebackCount")
                        ).as("chargebackResults")

                        // Pipeline 4: fraudRefundedCount for FRAUD_REFUNDED with isSettlementPending != 0
                        .and(
                                Aggregation.match(
                                        new Criteria().andOperator(
                                                Criteria.where("status").is("FRAUD_REFUNDED"),
                                                Criteria.where("isSettlementPending").ne(0),
                                                new Criteria().andOperator(commonFilters.toArray(new Criteria[0]))
                                        )
                                ),
                                Aggregation.group().count().as("fraudRefundedCount")
                        ).as("fraudRefundedResults")
        );

        Aggregation aggregation = Aggregation.newAggregation(operations);
        System.out.println("getRefundedOrChargebackAmount : " + " "+aggregation.toString() );
        AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, "purchase", Document.class);
        System.out.println("getRefundedOrChargebackAmount : "  +  results );
        Document result = results.getUniqueMappedResult();

        float totalSum = 0.0f;
        int refundedCount = 0;
        int chargebackCount = 0;
        int fraudRefundedCount = 0;

        if (result != null) {
            // Extract from sumResults
            List<Document> sumList = (List<Document>) result.get("sumResults");
            if (!sumList.isEmpty()) {
                Double sum = sumList.get(0).getDouble("totalSum");
                totalSum = sum != null ? sum.floatValue() : 0.0f;
            }

            // Extract from refundedResults
            List<Document> refundedList = (List<Document>) result.get("refundedResults");
            if (!refundedList.isEmpty()) {
                Integer count = refundedList.get(0).getInteger("refundedCount");
                refundedCount = count != null ? count : 0;
            }

            // Extract from chargebackResults
            List<Document> chargebackList = (List<Document>) result.get("chargebackResults");
            if (!chargebackList.isEmpty()) {
                Integer count = chargebackList.get(0).getInteger("chargebackCount");
                chargebackCount = count != null ? count : 0;
            }

            List<Document> fraudRefundedList = (List<Document>) result.get("fraudRefundedResults");
            if (!fraudRefundedList.isEmpty()) {
                Integer count = fraudRefundedList.get(0).getInteger("fraudRefundedCount");
                fraudRefundedCount = count != null ? count : 0;
            }
        }

        // Replace totalRollingReserve with 0, and use refunded/chargeback counts
        return new AggregateTransactionsResult(totalSum, 0,0,fraudRefundedCount, refundedCount, chargebackCount);
    }


}
