(ns tasks
  (:require [babashka.fs :as fs]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [babashka.tasks :as tasks]
            [babashka.process :as pr]
            [cheshire.core :as json]
            [selmer.parser :as template]
            [clojure.math :refer [floor round log pow]]
            [clojure.tools.logging :as log]))

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

;; bb psql-prompt --conn local

(defn psql-prompt [{:keys [conn] :as params}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/error "Connection" conn "not found") (exit 1))
    (let [command (string/join " " `["psql" ~@(format-conn c)])]
      (tasks/shell {:extra-env {:PGPASSWORD (:pass c)}} command))))

(defn- psql-command [conn command]
  `["psql" ~@(format-conn conn) "--command" ~command])

;; bb psql-command --conn local

(defn psql-command* [{:keys [conn]}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/error "Connection " conn " not found") (exit 1))
    (let [command (psql-command c "")]
      (println (string/join " " command)))))

;; bb copy --conn eu-test --to data --query 'select * from foo limit 10'
;; bb copy --conn local --to data --table foo
;; bb copy --conn eu-test --to data --query "select * from foo where collection_date > now() - '2 days'::interval"

(defn copy [{:keys [conn to file table query from-csv]}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/error "Connection" conn " not found") (exit 1))
    (let [command (psql-command
                   c (cond
                       from-csv (format "\\copy %s from %s" table from-csv) ;;(format "\\copy %s from %s with (delimiter E'\\t', format csv)" table from-csv)
                       table    (format "\\copy %s to %s" table to)
                       file     (format "\\copy %s to %s" file to) ;; TODO not sure this is valid
                       query    (format "\\copy (%s) to %s" query to)))]
      ;;(prn command)
      (-> (pr/process command {:extra-env {:PGPASSWORD (:pass c)}})
          (pr/check)))))

;; bb dump --conn local --to clusters.sql --table foo

(defn dump [{:keys [conn to table schema] :as params}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/error "Connection" conn "not found") (exit 1))
    (let [command `["pg_dump"
                    ~@(format-conn c)
                    ~@(cond
                        table  ["--table" table]
                        schema ["--schema" schema])]
          p       (-> (pr/process command (merge
                                           {:extra-env {:PGPASSWORD (:pass c)}}
                                           (when to
                                             {:out       :write
                                              :out-file  (io/file to)})))
                      (pr/check))]
      (when-not to
        (-> p :out slurp println))
      p)))

;; bb dump-schema --conn local --to schema.sql --schema public
;; bb dump-schema --conn local --to schema.sql --table foo
;; bb dump-schema --conn local --to schema.sql --table 'foo__*'
;; bb dump-schema --conn local --to schema.sql

(defn dump-schema [{:keys [conn to table schema] :as params}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/error "Connection" conn "not found") (exit 1))
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

;; bb eval --conn local --command 'select * from foo'
;; bb eval --conn local --file schema.sql

(defn- format-opts [{:keys [quiet tuples-only no-align no-psqlrc] :as opts}]
  (->> opts
       (map name)
       (map (partial str "--"))))

(defn psql-eval [{:keys [conn file to command capture opts] :as params}]
  ;;(prn params)
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/error "Connection" conn "not found") (exit 1))
    (let [command (cond
                    file    `["psql" ~@(format-conn c) ~@(format-opts opts) "--file" ~file]
                    command `["psql" ~@(format-conn c) ~@(format-opts opts) "--command" ~command])
          p       (-> (pr/process command (merge
                                           {:extra-env {:PGPASSWORD (:pass c)}}
                                           (when to
                                             {:out       :write
                                              :out-file  (io/file to)}))))]
      (when (and (not to) (not capture))
        (-> p :out slurp println))
      (if capture
        (-> p :out slurp)
        (do
          (pr/check p)
          p)))))

(defn- temp-file []
  (str (fs/absolutize (fs/create-temp-file))))

;; bb copy-schema --conn eu-test --to local --table foo

(defn copy-schema [{:keys [conn to table schema] :as params}]
  (let [file (temp-file)]
    (log/info "Dumping schema to temp file" file)
    (dump-schema (-> params
                     (assoc :to file)))
    (log/info "Applying schema...")
    (psql-eval {:conn to :file file :opts [:quiet :tuples-only :no-align :no-psqlrc]})))

(defn- table-exists? [{:keys [conn table]}]
  (-> (psql-eval {:conn conn :capture true
                  :opts [:quiet :tuples-only :no-align :no-psqlrc]
                  :command (str "select json_agg(to_regclass('" table "'));")})
      json/parse-string
      first
      some?))

(defn- glob-tables [{:keys [conn pattern]}]
  (-> (psql-eval {:conn conn :capture true
                  :opts [:quiet :tuples-only :no-align :no-psqlrc]
                  :command (str "select json_agg(table_name) from information_schema.tables where table_name like '" pattern "';")})
      json/parse-string
      sort))

(defn- logn [num base]
  (/ (round (log num))
     (round (log base))))

(defn- human-filesize
  "Format a number of bytes as a human readable filesize (eg. 10 kB). By
   default, decimal suffixes (kB, MB) are used.  Passing :binary true will use
   binary suffixes (KiB, MiB) instead.

  Adapted from https://github.com/clj-commons/humanize"
  [bytes & {:keys [binary fmt]
            :or   {binary false
                   fmt "%.1f"}}]

  (if (zero? bytes)
    ;; special case for zero
    "0 bytes"
    (let [decimal-sizes [:B, :KB, :MB, :GB, :TB,
                         :PB, :EB, :ZB, :YB]
          binary-sizes  [:B, :KiB, :MiB, :GiB, :TiB,
                         :PiB, :EiB, :ZiB, :YiB]

          units         (if binary binary-sizes decimal-sizes)
          base          (if binary 1024 1000)

          base-pow      (int (floor (logn bytes base)))
          ;; if base power shouldn't be larger than biggest unit
          base-pow      (if (< base-pow (count units))
                          base-pow
                          (dec (count units)))
          suffix        (name (get units base-pow))
          value         (float (/ bytes (pow base base-pow)))]

      (str (format fmt value) suffix))))

;; bb copy-data --conn eu-prod --to local --truncate --table foo
;; bb copy-data --conn eu-prod --to local --truncate --table-pattern 'foo%'
;; bb copy-data --conn eu-prod --to local --truncate --table-pattern 'foo%' --query-template "select * from {{table}} where date='2023-02-11'"

(defn copy-data [{:keys [conn to table table-pattern query query-template truncate drop]}]
  (if table-pattern
    (let [tables (glob-tables {:conn conn :pattern table-pattern})]
      (if (empty? tables)
        (log/errorf "Did not find any tables matching %s" table-pattern)
        (do
          (log/infof "Matched %d tables: %s" (count tables) (string/join "," tables))
          (doseq [table tables]
            (if query-template
              (copy-data {:conn conn :to to :table table :truncate truncate
                          :query (template/render query-template {:table table})})
              (copy-data {:conn conn :to to :table table :truncate truncate}))))))
    (let [file (temp-file)]
      (log/infof "Copying data from %s to temp file %s" (or query table) file)
      (copy (merge {:conn conn :to file}
                   (if query
                     {:query query}
                     {:table table})))

      (if (table-exists? {:conn to :table table})
        (cond
          drop
          (do
            (log/infof "Table %s exists in destination database, dropping..." table)
            (psql-eval {:conn to :opts [:quiet] :command (str "drop table " table)}))

          truncate
          (do
            (log/infof "Table %s exists in destination database, truncating..." table)
            (psql-eval {:conn to :opts [:quiet] :command (str "truncate table " table)}))

          :else
          (log/infof "Table %s exists in destination database, will append data to it..." table))
        (do
          (log/infof "Table %s does not exist in destination database, copying schema..." table)
          (copy-schema {:conn conn :to to :table table})))

      (log/infof "Copying data from temp file %s (%s) to table %s" file (human-filesize (fs/size file)) table)
      (copy {:conn to :from-csv file :table table}))))
