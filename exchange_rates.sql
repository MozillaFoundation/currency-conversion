create table exchange_rates(
							 base_currency VARCHAR(5) NOT NULL DEFAULT 'USD'
                            ,local_currency VARCHAR(5) NOT NULL
                            ,exchange_rate_date date NOT NULL
                            ,exchange_rate_updated_date timestamptz NOT NULL
                            ,exchange_rate decimal(24,8) NOT NULL
                            ,period VARCHAR(255) NOT NULL
                            ,source_api_host VARCHAR(255) NOT NULL
                            ,source_api_path VARCHAR(255) NOT NULL 
							,primary key (local_currency, exchange_rate_date)
							);