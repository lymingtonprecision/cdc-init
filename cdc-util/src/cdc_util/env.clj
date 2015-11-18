(ns cdc-util.env)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn env->config
  "Given a map of environment variables and collection of keys or
  tuples of `[:env-key :config-key]` returns a map of selected
  configuration values:

      (env->config
       {:db-name \"app-db\"
        :db-host \"db.example.com\"
        :db-user \"admin\"
        :password \"my-secret\"
        :home \"/users/example\"
        ;; ... other env vars
        }
       [[:db-name :database]
        [:db-host :server]
        [:db-user :username]
        :password])
      ;=> {:database \"app-db\"
      ;;   :server   \"db.example.com\"
      ;;   :username \"admin\"
      ;;   :password \"my-secret\"}"
  [env config-keys]
  (reduce
   (fn [rs k]
     (if (coll? k)
       (assoc rs (second k) (get env (first k)))
       (assoc rs k (get env k))))
   {}
   config-keys))
