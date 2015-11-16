(ns cdc-init.components.util
  "Various utility fns related to component creation/use.")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn extract-options-from-env
  "Given a map of environment variables and another of `env-var ->
  option-name` mappings returns a map of the environment variable
  values mapped to their corresponding option names.

      (extract-options-from-env
       {:db-host \"db.example.com\"
        :db-user \"admin\"
        :home \"/users/example\"}
       {:db-host :hostname
        :db-user :username})
      ;=> {:hostname \"db.example.com\" :username \"admin\"}"
  [env env-key->option-map]
  (reduce
   (fn [opts [k v]]
     (if-let [k (env-key->option-map k)]
       (assoc opts k v)
       opts))
   {}
   env))
