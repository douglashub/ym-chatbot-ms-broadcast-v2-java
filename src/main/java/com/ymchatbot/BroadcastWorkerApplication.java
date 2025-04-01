package com.ymchatbot;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import jakarta.annotation.PostConstruct;

@SpringBootApplication
public class BroadcastWorkerApplication {
    
    @Autowired
    private WorkerSendMessage workerSendMessage;
    
    @Autowired
    private WorkerUpdateStatus workerUpdateStatus;
    
    public static void main(String[] args) {
        SpringApplication.run(BroadcastWorkerApplication.class, args);
    }
    
    @PostConstruct
    public void startWorkers() throws Exception {
        // Inicializar e iniciar os workers após a criação do contexto Spring
        workerSendMessage.start();
        workerUpdateStatus.start();
        
        // Log de inicialização
        LoggerUtil.info("Broadcast workers initialized and started successfully");
    }
}