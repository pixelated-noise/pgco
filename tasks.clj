(ns tasks
  (:require [babashka.fs :as fs]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [babashka.tasks :as tasks]
            [babashka.process :as pr]
            [clojure.tools.logging.readable :as log]))

(defn- exit [ret]
  (System/exit ret))

(defn- load-config []
  (-> (str (fs/home) "/.pgco/config.edn")
      (slurp)
      (edn/read-string)))

(defn- format-conn [{:keys [host db user port]}]
  ["--host" host
   "--dbname" db
   "--username" user
   "--port" (str port)])

;; bb psql --conn bsq-local

(defn psql-prompt [{:keys [conn] :as params}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/fatalf "Connection %s not found" conn) (exit 1))
    (let [command (string/join " " `["psql" ~@(format-conn c)])]
      (tasks/shell {:extra-env {:PGPASSWORD (:pass c)}} command))))

(defn- psql-command [conn command]
  `["psql" ~@(format-conn conn) "--command" ~command])

;; bb copy --conn bsq-eu-test --to data --query 'select * from sonygwt_aa_webkpi_eu__extract limit 10'
;; bb copy --conn bsq-local --to data --table continental_ga_standard_global__cluster_sessions_h
;; bb copy --conn bsq-eu-test --to data --query "select * from sonygwt_aa_webkpi_eu__extract where collection_date > now() - '2 days'::interval"

(defn copy [{:keys [conn to file table query from-csv]}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/fatalf "Connection %s not found" conn) (exit 1))
    (let [command (psql-command
                   c (cond
                       from-csv (format "\\copy %s from %s with (delimiter E'\\t', format csv)" table from-csv)
                       table    (format "\\copy %s to %s" table to)
                       file     (format "\\copy %s to %s" file to) ;; TODO not sure this is valid
                       query    (format "\\copy (%s) to %s" query to)))]
      (prn command)
      (-> (pr/process command {:extra-env {:PGPASSWORD (:pass c)}})
          (pr/check)))))

;; bb dump-schema --conn bsq-local --to schema.sql --schema public
;; bb dump-schema --conn bsq-local --to schema.sql --table continental_ga_standard_global__cluster_sessions_h
;; bb dump-schema --conn bsq-local --to schema.sql --table 'continental_ga_standard_global__*'
;; bb dump-schema --conn bsq-local --to schema.sql
;; bb dump-schema --conn bsq-eu-test --to schema.sql --table 'domes_ga_me_eu__*'

(defn dump-schema [{:keys [conn to table schema] :as params}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/fatalf "Connection %s not found" conn) (exit 1))
    (let [command `["pg_dump"
                    ~@(format-conn c)
                    ~@(cond
                        table  ["--schema-only" "--table" table]
                        schema ["--schema-only" "--schema" schema]
                        :else  ["--schema-only"])]
          p       (-> (pr/process command (merge
                                           {:extra-env {:PGPASSWORD (:pass c)}}
                                           (when to
                                             {:out       :write
                                              :out-file  (io/file to)})))
                      (pr/check))]
      (when-not to
        (-> p :out slurp println))
      p)))

;; bb eval --conn bsq-local --command 'select * from www1800contacts_aav2_ca_us_app__extract'
;; bb eval --conn bsq-local --file schema.sql

(defn psql-eval [{:keys [conn file to command] :as params}]
  (prn params)
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/fatalf "Connection %s not found" conn) (exit 1))
    (let [command (cond
                    file    `["psql" ~@(format-conn c) "--file" ~file]
                    command `["psql" ~@(format-conn c) "--command" ~command])
          p       (-> (pr/process command (merge
                                           {:extra-env {:PGPASSWORD (:pass c)}}
                                           (when to
                                             {:out       :write
                                              :out-file  (io/file to)}))))]
      (when-not to
        (-> p :out slurp println))
      (pr/check p)
      p)))

(defn- temp-file []
  (str (fs/absolutize (fs/create-temp-file))))

;; bb copy-schema --conn bsq-eu-test --to bsq-local --table baresquare_ga_custom_011lt__full_ticket
;; bb copy-schema --conn bsq-eu-test --to bsq-local --table 'domes_ga_me_eu__*'

(defn copy-schema [{:keys [conn to table schema] :as params}]
  (let [file (temp-file)]
    (log/info "Dumping schema to temp file" file)
    (dump-schema (-> params
                     (assoc :to file)))
    (log/info "Applying schema...")
    (psql-eval {:conn to :file file})))
