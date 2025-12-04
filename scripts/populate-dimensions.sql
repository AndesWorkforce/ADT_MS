-- Script para poblar las tablas de dimensiones con valores iniciales
-- Basado en PRODUCTIVITY_SCORE.md

-- Poblar apps_dimension
INSERT INTO apps_dimension (app_name, category, weight) VALUES
  -- Productivas
  ('Code', 'productive', 1.2),
  ('Visual Studio Code', 'productive', 1.2),
  ('IntelliJ', 'productive', 1.2),
  ('Word', 'productive', 1.0),
  ('Excel', 'productive', 1.0),
  ('PowerPoint', 'productive', 1.0),
  -- Neutras
  ('Slack', 'neutral', 0.8),
  ('Teams', 'neutral', 0.8),
  ('Chrome', 'neutral', 0.6),
  ('Edge', 'neutral', 0.6),
  -- No productivas
  ('YouTube', 'non_productive', 0.2),
  ('Spotify', 'non_productive', 0.3),
  ('Discord', 'non_productive', 0.4),
  ('Games', 'non_productive', 0.1);

-- Poblar domains_dimension
INSERT INTO domains_dimension (domain, category, weight) VALUES
  -- Productivos
  ('github.com', 'productive', 1.3),
  ('stackoverflow.com', 'productive', 1.2),
  ('atlassian.net', 'productive', 1.1),
  ('jira.', 'productive', 1.1),  -- Prefijo para match (ej: jira.company.com)
  ('confluence.', 'productive', 1.1),  -- Prefijo para match
  ('docs.google.com', 'productive', 1.0),
  ('notion.so', 'productive', 1.0),
  -- Neutros
  ('google.com', 'neutral', 0.7),
  ('bing.com', 'neutral', 0.7),
  ('extensions', 'neutral', 0.5),
  -- No productivos
  ('youtube.com', 'non_productive', 0.2),
  ('facebook.com', 'non_productive', 0.1),
  ('twitter.com', 'non_productive', 0.2),
  ('instagram.com', 'non_productive', 0.1),
  ('reddit.com', 'non_productive', 0.3);

