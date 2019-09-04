package com.bustanilarifin;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        ActorSystem actorSystem = ActorSystem.create("payment");

        ActorRef paymentProcessor = actorSystem.actorOf(PaymentProcessor.props(), "payment-processor");

        for (int i = 0; i < 20; i++) {
            paymentProcessor.tell(new PaymentProcessor.ProcessPayment("request-" + i, System.currentTimeMillis()), ActorRef.noSender());
        }
        paymentProcessor.tell(PoisonPill.getInstance(), ActorRef.noSender());
        System.in.read();
        actorSystem.terminate();
    }
}

class PaymentProcessor extends AbstractActor {

    private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    long start;

    @Getter
    @AllArgsConstructor
    @ToString
    public static class ProcessPayment {
        private String requestId;
        private long startMillis;
    }

    @Override public void preStart() throws Exception {
        start = System.currentTimeMillis();
    }

    @Override public void postStop() throws Exception {
        System.out.println(">>> " + (System.currentTimeMillis() - start) + "<<<<");
    }

    public static Props props() {
        return Props.create(PaymentProcessor.class, PaymentProcessor::new);
    }

    @Override public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(BillerRequestSuccessful.class, billerRequestSuccessful -> {
                    ActorRef notifier = getContext().actorOf(Notifier.props());
                    notifier.tell(new Notifier.Notify(billerRequestSuccessful.requestId, "Payment successful"), getSelf());
                })
                .match(BalanceDeducted.class, balanceDeducted -> {
                    ActorRef backend = getContext().actorOf(BillerConnector.props());
                    backend.tell(new BillerConnector.SendRequest(balanceDeducted.requestId), getSelf());
                })
                .match(ProcessPayment.class, processPayment -> {
                    log.info("Processing payment: {}", processPayment.requestId);
                    ActorRef backend = getContext().actorOf(BackendActor.props());
                    backend.tell(new BackendActor.DeductBalance(processPayment.requestId), getSelf());
                })
                .build();
    }

    @Getter
    @AllArgsConstructor
    @ToString
    public static class BalanceDeducted {
        private String requestId;
    }

    @Getter
    @AllArgsConstructor
    @ToString
    public static class BillerRequestSuccessful {
        private String requestId;
    }
}

class BillerConnector extends AbstractActor {

    private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    @AllArgsConstructor
    @Getter
    @ToString
    public static class SendRequest {
        private String requestId;
    }

    public static Props props() {
        return Props.create(BillerConnector.class, BillerConnector::new);
    }

    @Override public Receive createReceive() {
        return ReceiveBuilder.create().match(SendRequest.class, sendRequest -> {
            log.info("Sending request to biller for id {}", sendRequest.requestId);
            Thread.sleep(5000);
            log.info("done sending request {} to biller", sendRequest.requestId);
            getSender().tell(new PaymentProcessor.BillerRequestSuccessful(sendRequest.requestId), getSelf());
        }).build();
    }
}

class BackendActor extends AbstractActor {

    private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props() {
        return Props.create(BackendActor.class, BackendActor::new);
    }

    @AllArgsConstructor
    @Getter
    @ToString
    public static class DeductBalance {
        private String requestId;
    }

    @Override public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(DeductBalance.class, deductBalance -> {
                    log.info("Deducting balance for {}", deductBalance.requestId);
                    Thread.sleep(3000);
                    log.info("done!");
                    getSender().tell(new PaymentProcessor.BalanceDeducted(deductBalance.requestId), getSelf());
                }).build();
    }
}

class Notifier extends AbstractActor {

    private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props() {
        return Props.create(Notifier.class, Notifier::new);
    }

    @Getter
    @AllArgsConstructor
    @ToString
    public static class Notify {
        private String requestId;
        private String message;
    }

    @Override public Receive createReceive() {
        return ReceiveBuilder.create().match(Notify.class, notify -> {
            log.info(notify.message + " [{}]", notify.requestId);
        }).build();
    }
}
