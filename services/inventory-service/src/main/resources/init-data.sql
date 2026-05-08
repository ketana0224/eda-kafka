-- 初期商品データ（検証用）
-- Hibernate の ddl-auto=update で products テーブルが作成された後に手動で実行するか、
-- data.sql として Spring Boot に自動実行させる

INSERT INTO products (product_id, name, stock_quantity) VALUES
  ('PROD-001', 'ノートPC',        10),
  ('PROD-002', 'ワイヤレスマウス', 50),
  ('PROD-003', 'USBハブ',         30),
  ('PROD-004', 'モニター',         5),
  ('PROD-005', 'キーボード',       20)
ON CONFLICT (product_id) DO NOTHING;
