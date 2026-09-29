package com.marcosfurquim.iotcase.api;

import com.marcosfurquim.iotcase.importer.CsvEventReader;
import com.marcosfurquim.iotcase.importer.CsvImportService;
import com.marcosfurquim.iotcase.processing.ProcessedEventRepository;
import java.io.IOException;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class CaseController {
    private final CsvImportService importer;
    private final ProcessedEventRepository repository;

    public CaseController(CsvImportService importer, ProcessedEventRepository repository) {
        this.importer = importer;
        this.repository = repository;
    }

    @PostMapping("/imports/sample")
    public CsvEventReader.ReadResult importSample() throws IOException {
        return importer.importConfiguredCsv();
    }

    @GetMapping("/processing/summary")
    public Map<String, Long> summary() {
        return repository.countByType();
    }

    @ExceptionHandler(IOException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> missingCsv(IOException exception) {
        return Map.of("error", "CSV configurado não encontrado ou ilegível");
    }
}
