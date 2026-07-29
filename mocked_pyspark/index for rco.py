sequenceDiagram
    autonumber

    box rgb(235,245,255) NPCO
        participant FE as Front-end
        participant BE as Back-end
        participant OCRMS as Microserviço OCR
        participant LLMMS as Microserviço LLM
        participant Mongo as MongoDB
    end

    box rgb(250,250,250) Bridge
        participant OCRAPI as API OCR
        participant LLMAPI as API LLM
    end

    FE->>BE: POST /analysis (arquivo)

    BE->>OCRMS: Envia arquivo
    OCRMS->>OCRAPI: OCR do documento
    OCRAPI-->>OCRMS: Texto extraído
    OCRMS-->>BE: Resultado OCR

    BE->>LLMMS: Texto OCR + instruções
    LLMMS->>LLMAPI: Processamento LLM
    LLMAPI-->>LLMMS: Resultado estruturado
    LLMMS-->>BE: Resultado da análise

    BE->>Mongo: Salva análise

    BE-->>FE: Ticket de processamento

    loop Polling
        FE->>BE: GET /analysis/{ticket}
        alt Processando
            BE-->>FE: PROCESSING
        else Concluído
            BE->>Mongo: Busca resultado
            Mongo-->>BE: Dados
            BE-->>FE: COMPLETED + Resultado
        end
    end