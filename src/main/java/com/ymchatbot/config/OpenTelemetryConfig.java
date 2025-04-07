package com.ymchatbot.config;

import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.api.trace.Tracer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.concurrent.TimeUnit;

@Configuration
public class OpenTelemetryConfig {

    @Value("${opentelemetry.jaeger.endpoint:http://jaeger:4317}") // A porta padrão OTLP gRPC para Jaeger é 4317
    private String jaegerEndpoint;

    @Bean
    public OpenTelemetry openTelemetry() {
        // Cria o exportador OTLP via gRPC para enviar spans para o Jaeger
        OtlpGrpcSpanExporter otlpExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(jaegerEndpoint)
                .setTimeout(30, TimeUnit.SECONDS)
                .build();

        // Cria um processador de spans com base em lotes
        BatchSpanProcessor spanProcessor = BatchSpanProcessor.builder(otlpExporter)
                .build();

        // Cria um Resource com informações do serviço
        Resource serviceResource = Resource.create(Attributes.of(
                ResourceAttributes.SERVICE_NAME, "ym-chatbot-service",
                ResourceAttributes.SERVICE_VERSION, "1.0.0",
                ResourceAttributes.DEPLOYMENT_ENVIRONMENT, "production"
        ));

        // Configura o SdkTracerProvider com o processador e o Resource
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(Resource.getDefault().merge(serviceResource))
                .setSampler(Sampler.alwaysOn())
                .addSpanProcessor(spanProcessor)
                .build();

        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();

        return openTelemetry;
    }

    @Bean
    public Tracer tracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer("ym-chatbot-tracer", "1.0.0");
    }
}
