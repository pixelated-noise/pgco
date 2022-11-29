(ns tasks
  (:require [babashka.fs :as fs]
            [clojure.edn :as edn]
            [clojure.string :as string]
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
  ["-h" host
   "-d" db
   "-U" user
   "-p" (str port)])

;; bb psql --conn bsq-local

(defn psql-prompt [{:keys [conn] :as params}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/fatalf "Connection %s not found" conn) (exit 1))
    (let [command (string/join " " `["psql" ~@(format-conn c)])]
      (log/info command)
      (tasks/shell {:extra-env {:PGPASSWORD (:pass c)}} command))))

;; bb copy --conn bsq-eu-test --to data --query 'select * from sonygwt_aa_webkpi_eu__extract limit 10'
;; bb copy --conn bsq-local --to data --table continental_ga_standard_global__cluster_sessions_h
;; bb copy --conn bsq-eu-test --to data --query "select * from sonygwt_aa_webkpi_eu__extract where collection_date > now() - '2 days'::interval"

(defn- psql-command [conn command]
  `["psql" ~@(format-conn conn) "--command" ~command])

(defn copy [{:keys [conn to file table query]}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])]
    (when-not c (log/fatalf "Connection %s not found" conn) (exit 1))
    (let [command (psql-command
                   c (cond
                       table (format "\\copy %s to %s" table to)
                       file  (format "\\copy %s to %s" file to)
                       query (format "\\copy (%s) to %s" query to)))]
      (log/info command)
      (-> (pr/process command {:extra-env {:PGPASSWORD (:pass c)}})
          (pr/check)))))

(defn copy-schema [{:keys [conn dest table]}]
  (let [config (load-config)
        c      (get-in config [:connections (keyword conn)])
        d      (get-in config [:connections (keyword dest)])]
    (when-not c (log/fatalf "Connection %s not found" conn) (exit 1))
    (when-not d (log/fatalf "Connection %s not found" dest) (exit 1))
))
