package com.example;

public class Main
{
    public static void main( String[] args )
    {
        while (true) {
            TaskQueueService taskQueueService = new TaskQueueService();
            System.out.println(taskQueueService.getTask());
        }
    }
}
