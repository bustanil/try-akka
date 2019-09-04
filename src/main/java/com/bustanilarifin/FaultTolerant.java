package com.bustanilarifin;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;

public class FaultTolerant {

    public static void main(String[] args) throws IOException {
        ActorSystem system = ActorSystem.create("fault-tolerant-test");
        ActorRef faulty = system.actorOf(FaultyActor.props());
        for (int i = 1; i <= 20; i++) {
            System.out.println("Sending message #" + i);
            if (i % 5 == 0) {
                faulty.tell(new FaultyActor.Message(null), ActorRef.noSender());
            } else {
                faulty.tell(new FaultyActor.Message(String.valueOf(i)), ActorRef.noSender());
            }
        }
        System.in.read();
        system.terminate();
    }

}

class FaultyActor extends AbstractActor {

    public static Props props() {
        return Props.create(FaultyActor.class, FaultyActor::new);
    }

    @Override public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(Message.class, message -> {
                    if (message.getContent() == null) {
                        throw new RuntimeException("Something wrong");
                    }
                    System.out.print("processing message #" + message.getContent());
                    Thread.sleep(3000);
                    System.out.println(" (Done)");
                })
                .build();
    }

    @AllArgsConstructor
    @Getter
    public static class Message {
        private String content;
    }
}
