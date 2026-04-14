create extension if not exists "pgcrypto";

create table if not exists bdstt_users (
  id uuid primary key default gen_random_uuid(),
  phone text,
  email text,
  telegram_chat_id text,
  telegram_username text,
  full_name text,
  consent_status text default 'unknown',
  source_channel text,
  created_at timestamptz default now()
);

create table if not exists bdstt_properties (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references bdstt_users(id) on delete set null,
  property_type text not null default 'apartment',
  city text,
  district text,
  ward text,
  project_name text,
  block_name text,
  area_net numeric,
  bedrooms int,
  bathrooms int,
  floor_no int,
  facing text,
  furnishing_level text,
  legal_status text,
  occupancy_status text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists bdstt_sale_calculations (
  id uuid primary key default gen_random_uuid(),
  property_id uuid not null references bdstt_properties(id) on delete cascade,
  session_id text not null unique,
  input_sale_price numeric,
  input_brokerage_mode text,
  input_brokerage_value numeric,
  input_loan_outstanding numeric,
  target_net_proceeds numeric,
  estimated_pit_tax numeric,
  estimated_brokerage_fee numeric,
  estimated_notary_fee numeric,
  estimated_other_costs numeric,
  estimated_net_proceeds numeric,
  result_json jsonb,
  created_at timestamptz default now()
);

create table if not exists bdstt_lead_events (
  id bigserial primary key,
  session_id text,
  user_id uuid,
  property_id uuid,
  event_type text not null,
  event_value text,
  event_meta_json jsonb,
  created_at timestamptz default now()
);

create table if not exists bdstt_telegram_links (
  id bigserial primary key,
  session_id text not null,
  link_token text not null unique,
  user_id uuid,
  telegram_chat_id text,
  linked_at timestamptz,
  status text default 'pending'
);
