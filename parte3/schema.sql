-- ============================================
-- SCHEMA DO BANCO DE DADOS ANS_DESPESAS
-- ============================================

-- Remover banco se existir e criar novamente
DROP DATABASE IF EXISTS ans_despesas;
CREATE DATABASE ans_despesas 
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

USE ans_despesas;

-- ============================================
-- TABELA: operadoras
-- Armazena informações cadastrais das operadoras
-- ============================================
CREATE TABLE operadoras (
    cnpj VARCHAR(14) PRIMARY KEY,
    registro_ans VARCHAR(6) NULL,
    razao_social VARCHAR(255) NOT NULL,
    modalidade VARCHAR(100) NULL,
    uf CHAR(2) NULL,
    INDEX idx_razao_social (razao_social),
    INDEX idx_uf (uf),
    INDEX idx_modalidade (modalidade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- TABELA: despesas_consolidadas
-- Armazena despesas consolidadas por trimestre
-- ============================================
CREATE TABLE despesas_consolidadas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cnpj VARCHAR(14) NOT NULL,
    razao_social VARCHAR(255) NOT NULL,
    ano INT NOT NULL,
    trimestre INT NOT NULL,
    valor_despesas DECIMAL(15,2) NOT NULL,
    FOREIGN KEY (cnpj) REFERENCES operadoras(cnpj) ON DELETE CASCADE,
    INDEX idx_ano_trimestre (ano, trimestre),
    INDEX idx_cnpj_periodo (cnpj, ano, trimestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- TABELA: despesas_agregadas
-- Armazena despesas agregadas por operadora/UF
-- ============================================
CREATE TABLE despesas_agregadas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cnpj VARCHAR(14) NULL,
    razao_social VARCHAR(255) NOT NULL,
    uf CHAR(2) NOT NULL,
    total_despesas DECIMAL(15,2) NOT NULL,
    media_despesas DECIMAL(15,2) NOT NULL,
    desvio_padrao DECIMAL(15,2) NULL,
    total_trimestres INT NOT NULL,
    INDEX idx_uf (uf),
    INDEX idx_total_despesas (total_despesas),
    INDEX idx_razao_uf (razao_social, uf)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
