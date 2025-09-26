-- Opcional: modo estricto recomendado

-- Crea y usa la base de datos
USE scikey;

-- Tabla principal de documentos
CREATE TABLE IF NOT EXISTS documents (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  doc_id BIGINT NOT NULL,
  title VARCHAR(255),
  abstract VARCHAR(1000),
  PRIMARY KEY (id),
  UNIQUE KEY uk_documents_doc_id (doc_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Organismos (lo creo antes por posibles FKs)
CREATE TABLE IF NOT EXISTS organisms (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  organismId_i BIGINT,
  organism_s VARCHAR(255),
  authOrganism_text VARCHAR(255),
  PRIMARY KEY (id),
  KEY idx_organismId_i (organismId_i)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS versions (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  doc_id BIGINT,
  producedDate_tdate DATE,
  submittedDate_tdate DATE,
  PRIMARY KEY (id),
  KEY idx_versions_doc_id (doc_id)
  -- Si quieres FK a documents(doc_id), deja el UNIQUE de arriba y descomenta:
  -- ,CONSTRAINT fk_versions_doc
  --   FOREIGN KEY (doc_id) REFERENCES documents (doc_id)
  --   ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS authors (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  doc_id BIGINT,
  authFirstName_s VARCHAR(255),
  authFirstName_sci VARCHAR(255),
  authLastName_s VARCHAR(255),
  authLastName_sci VARCHAR(255),
  authQuality_s VARCHAR(255),
  organismId_i BIGINT,
  PRIMARY KEY (id),
  KEY idx_authors_doc_id (doc_id),
  KEY idx_authors_organismId_i (organismId_i)
  -- Opcional FK:
  -- ,CONSTRAINT fk_authors_doc
  --   FOREIGN KEY (doc_id) REFERENCES documents (doc_id)
  --   ON DELETE CASCADE ON UPDATE CASCADE
  -- ,CONSTRAINT fk_authors_org
  --   FOREIGN KEY (organismId_i) REFERENCES organisms (organismId_i)
  --   ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS journals (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  doc_id BIGINT,
  journalId_i BIGINT,
  journalIssn_s VARCHAR(255),
  PRIMARY KEY (id),
  KEY idx_journals_doc_id (doc_id),
  KEY idx_journals_journalId_i (journalId_i)
  -- Opcional FK a documents(doc_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS keywords (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  doc_id BIGINT,
  keyword_s VARCHAR(255),
  keyword_sci VARCHAR(255),
  keyword_t VARCHAR(255),
  PRIMARY KEY (id),
  KEY idx_keywords_doc_id (doc_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

 
CREATE TABLE IF NOT EXISTS identifiers (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  doc_id BIGINT,
  doi_s VARCHAR(255),
  halId_s VARCHAR(255),
  isbn VARCHAR(255),
  PRIMARY KEY (id),
  KEY idx_identifiers_doc_id (doc_id),
  KEY idx_identifiers_doi (doi_s),
  KEY idx_identifiers_halid (halId_s),
  KEY idx_identifiers_isbn (isbn)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

