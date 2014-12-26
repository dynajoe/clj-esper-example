(ns clj-esp.core
  (:gen-class)
  (:use clj-esp.esper))

(def market-data-event 
  (new-event "MarketDataEvent" 
             {"symbol" :string
              "price" :double
              "size" :int
              "side" :string}))

(def sides ["bid"])

(def symbols ["ABC"])

(def large-order-statement 
  "SELECT * FROM MarketDataEvent(size > 1000)")

(def average-order-value-statement
  (str "SELECT symbol, AVG(price * size) AS asize "
       "FROM MarketDataEvent.win:time(5 SEC) "
       "GROUP BY symbol " 
       "OUTPUT LAST EVERY 1 SECONDS"))

(def esp-service 
  (create-service "MarketAnalysis"
                  (configuration market-data-event)))

(defn demo 
  [service statement listener num-orders]
  (let [s (new-statement service statement)
        _ (add-listener s (create-listener listener))]
    (doseq [n (range num-orders)]
      (let [random-event {"symbol" (rand-nth symbols)
                          "price"  (double n)
                          "size"   (rand-int 1200)
                          "side"   (rand-nth sides)}]
         (send-event esp-service 
                     random-event 
                     "MarketDataEvent")
         (Thread/sleep 100)))
  (destroy-statement s)))

(defn large-order-handler 
  [new-events]
  (let [event (first new-events)
        [sym price size side] (map #(.get event %) ["symbol" "price" "size" "side"])]
    (println (str "Large" side "on" sym ": " size " shares at " (format "%.2f" price)))))

(defn average-order-value-handler
  [new-events] 
  (doseq [e new-events] 
    (println (str "Average order value for " (.get e "symbol") ": " (format "%.2f" (.get e "asize"))))))

(def bidding-up-statement
  (str "SELECT * "
       "FROM pattern [every ("
       "e1=MarketDataEvent(side = 'bid') -> "
       "e2=MarketDataEvent(e2.price > e1.price, side = e1.side, symbol = e1.symbol) -> "
       "e3=MarketDataEvent(e3.price > e2.price, side = e2.side, symbol = e2.symbol))]"))

(defn bidding-up-handler
  [events]
  (let [es (first events)
        [e1 e2 e3] (map #(.get es %) ["e1" "e2" "e3"])
        sym (.get e1 "symbol")]
    (println (str sym ": ") 
             (map #(format "%.2f" (.get % "price")) [e1 e2 e3]))))

(defn -main 
  [& args] (demo))