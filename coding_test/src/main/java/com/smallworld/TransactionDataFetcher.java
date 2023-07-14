package com.smallworld;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.smallworld.data.Transaction;

import javax.swing.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public class TransactionDataFetcher {

    private static final Type REVIEW_TYPE = new TypeToken<List<Transaction>>() {
    }.getType();

    List<Transaction> transactions;


    // Read JSON file & populate transactions in the list of transactions.
    public TransactionDataFetcher() {
        Gson gson = new Gson();
        try {
            JsonReader reader = new JsonReader(new FileReader("transactions.json"));
            transactions = gson.fromJson(reader, REVIEW_TYPE);

            for(Transaction transaction : transactions) {
                System.out.println(transaction.toString());
            }

        } catch(FileNotFoundException e) {
            JOptionPane.showMessageDialog(null, "file transactions.json doesn't exist", "File Not Found", JOptionPane.WARNING_MESSAGE);
        }
    }

    public static void main(String args[]) {

        TransactionDataFetcher dataFetcher = new TransactionDataFetcher();

        System.out.println("Total Amount =======================================");
        System.out.println("Total Amount: " + dataFetcher.getTotalTransactionAmount() + "\n");
        System.out.println("Amount by Sender Full Name =========================");
        System.out.println("Amount by Sender Full Name " + dataFetcher.getTotalTransactionAmountSentBy("Tom Shelby")+ "\n");

        System.out.println("Highest transaction amount: ========================");
        System.out.println("Highest transaction amount: " + dataFetcher.getMaxTransactionAmount()+ "\n");

        System.out.println("Unique Clients Count: " + "=========================");
        System.out.println("Unique Clients Count: " + dataFetcher.countUniqueClients()+ "\n");

        System.out.println("Is Any Issue Not Resolved: " + "====================");
        System.out.println("Is Any Issue Not Resolved: " + dataFetcher.hasOpenComplianceIssues("Aunt Polly")+ "\n");

        System.out.println("Transactions Map: " + "=============================");
        System.out.println("Transactions Map: " + dataFetcher.getTransactionsByBeneficiaryName()+ "\n");

        System.out.println("Unresolved Issue Ids: " + "=========================");
        System.out.println("Unresolved Issue Ids: " + dataFetcher.getUnsolvedIssueIds()+ "\n");

        System.out.println("Get Solved Issues Messages: " + "===================");
        System.out.println("Get Solved Issues Messages: " + dataFetcher.getAllSolvedIssueMessages()+ "\n");

        System.out.println("Decending Sorted order return top 3: " +"===========");
        System.out.println("Decending Sorted order return top 3: " + dataFetcher.getTop3TransactionsByAmount()+ "\n");
        dataFetcher.getTopSender();


    }



    /**
     * Returns the sum of the amounts of all transactions
     */
    public double getTotalTransactionAmount() {
        double totalAmount = transactions.stream()
                .map(transaction -> transaction.getAmount())
                .reduce(0.0, Double::sum);
        return totalAmount;
    }




    /**
     * Returns the sum of the amounts of all transactions sent by the specified client
     */
    public double getTotalTransactionAmountSentBy(String senderFullName) {
        double totalAmount = transactions.stream()
                .filter(transaction -> transaction.getSenderFullName().equalsIgnoreCase(senderFullName) )
                .map(transaction -> transaction.getAmount())
                .reduce(0.0, Double::sum);
        return totalAmount;
    }



    /**
     * Returns the highest transaction amount
     */
    public double getMaxTransactionAmount() {
        double maxAmount = transactions.stream()
                .max(Comparator.comparingDouble(Transaction::getAmount))
                .get().getAmount();
        return maxAmount;
    }

    /**
     * Counts the number of unique clients that sent or received a transaction
     */
    public long countUniqueClients() {
        List<String> senders = transactions.stream()
                .filter(distinctByKey(Transaction::getSenderFullName))
                .map(transaction -> transaction.getSenderFullName())
                .collect(Collectors.toList());
//        System.out.println("Senders: " + senders);

        List<String> beneficiaries = transactions.stream()
                .filter(distinctByKey(Transaction::getBeneficiaryFullName))
                .map(transaction -> transaction.getBeneficiaryFullName())
                .collect(Collectors.toList());
//        System.out.println("Beneficiaries: " + beneficiaries);

        Set<String> uniqueNames = new HashSet<>(senders);
        uniqueNames.addAll(beneficiaries);
//        System.out.println("Unique Clients: " + uniqueNames);
        return uniqueNames.size();
    }

    private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(keyExtractor.apply(t));
    }

    /**
     * Returns whether a client (sender or beneficiary) has at least one transaction with a compliance
     * issue that has not been solved
     */
    public boolean hasOpenComplianceIssues(String clientFullName) {
        boolean anyIssueResolved = transactions.stream()
                .filter(transaction -> transaction.getSenderFullName().equalsIgnoreCase(clientFullName)
                        || transaction.getBeneficiaryFullName().equalsIgnoreCase(clientFullName))
                .anyMatch(transaction -> transaction.isIssueSolved() == false);
        return anyIssueResolved;
    }

    /**
     * Returns all transactions indexed by beneficiary name
     */
    public Map<String, Transaction> getTransactionsByBeneficiaryName() {
        Map<String, Transaction> transactionMap = transactions.stream()
                .filter(distinctByKey(Transaction::getBeneficiaryFullName))
                .collect(toMap(Transaction::getBeneficiaryFullName, Function.identity()));
        return transactionMap;
    }

    /**
     * Returns the identifiers of all open compliance issues
     */
    public Set<Integer> getUnsolvedIssueIds() {
        Set<Integer> unresolvedIssueIds = transactions.stream()
                .filter(transaction -> transaction.isIssueSolved() == false)
                .map(transaction -> transaction.getIssueId())
                .collect(Collectors.toSet());
        return unresolvedIssueIds;
    }



    /**
     * Returns a list of all solved issue messages
     */
    public List<String> getAllSolvedIssueMessages() {

        List<String> solvedIssueMsg = transactions.stream()
                .filter(transaction -> transaction.isIssueSolved() == true)
                .map(transaction -> transaction.getIssueMessage())
                .collect(Collectors.toList());
        return solvedIssueMsg;
    }

    /**
     * Returns the 3 transactions with the highest amount sorted by amount descending
     */
    public List<Transaction> getTop3TransactionsByAmount() {

       List<Transaction> transaction = transactions.stream()
                .sorted((Transaction p01, Transaction p02)
                        -> (int) (p02.getAmount() - p01.getAmount())).limit(3).collect(Collectors.toList())
               ;

       // System.out.println("Returns Descending Sorted : "+  transaction);

        return(transaction);

    }




    /**
     * Returns the senderFullName of the sender with the most total sent amount
     */
  //  public Optional<String> getTopSender() {
    public void getTopSender() {

/*
        orders.stream()
                .collect(
                        Collectors.groupingBy(Order::getOrderMonth,
                                Collectors.collectingAndThen(
                                        Collectors.groupingBy(
                                                Order::getCustomer,
                                                Collectors.summarizingLong(Order::getAmount)
                                        ),
                                        e -> e.entrySet()
                                                .stream()
                                                .map(entry -> new BuyerDetails(entry.getKey(), entry.getValue().getSum()))
                                                .max(Comparator.comparingLong(BuyerDetails::getAmount))
                                )
                        )
                )*/

      /*  Map<Transaction, List<Transaction>> map = transactions.stream()
                .collect(Collectors.groupingBy(Transaction::getSenderFullName))
                .entrySet().stream()
                .collect(Collectors.toMap(e -> e.getValue().stream()
                                .max(Comparator.comparing(
                                        Transaction::getAmount))
                                .get(),
                        Map.Entry::getValue));*/

       // System.out.println("Sum Amount Group BY senderFull Name : " + transactionListMap );


            }
    }