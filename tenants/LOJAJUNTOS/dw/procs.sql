-- Exemplo simples: upsert de stg_clientes -> dw_clientes
CREATE TABLE IF NOT EXISTS dw_clientes (
  id BIGINT PRIMARY KEY,
  nome_completo VARCHAR(255),
  email VARCHAR(255),
  telefone VARCHAR(50),
  dt_carga DATETIME NOT NULL
);

DELIMITER //
CREATE OR REPLACE PROCEDURE prc_transform_clientes()
BEGIN
  INSERT INTO dw_clientes (id, nome_completo, email, telefone, dt_carga)
  SELECT id, nome_completo, email, telefone, NOW() FROM stg_clientes
  ON DUPLICATE KEY UPDATE
    nome_completo = VALUES(nome_completo),
    email         = VALUES(email),
    telefone      = VALUES(telefone),
    dt_carga      = VALUES(dt_carga);
  TRUNCATE TABLE stg_clientes;
END //
DELIMITER ;
