DROP TABLE IF EXISTS public.matsim CASCADE;

--Create ant_pos table and index
CREATE SEQUENCE public.matsim_id_seq
  INCREMENT 1
  MINVALUE 0
  MAXVALUE 9223372036854775807
  START 0
  CACHE 1;

CREATE TABLE public.matsim
(
  id integer NOT NULL DEFAULT nextval('trips_id_seq'::regclass),
  user_id integer,
  --commute_direction integer,
  --start_taz integer,
  --end_taz integer,
  linkpath int[],
  CONSTRAINT matsim_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX matsim_user_id_idx ON matsim
USING btree(user_id);