module MapReduce where

import Data.Ord
import Data.List

-- ---------------------------------Sección 1---------Diccionario ---------------------------
type Dict k v = [(k,v)]

-- Ejercicio 1
belongs :: Eq k => k -> Dict k v -> Bool

-- Resolución 1a : Función 'belongs' implementada con foldr. Se le pasa la función auxiliar
	-- 'comparar' definida a continuación y el valor False como caso base. Si la lista es
	-- vacía, devuelve siempre False.
belongs c = foldr (comparar c) False

	-- Función auxiliar 'comparar' utilizada para 'belongs'. Toma una clave 'c' del
		-- diccionario y devuelve una función que, dado un elemento del diccionario y un
		-- booleano, devuelve la conjunción entre el booleano por un lado y la comparación
		-- de la clave c con la clave del elemento por el otro.
comparar :: Eq k => k -> ( (k,v) -> Bool -> Bool )
comparar = (\c -> \t b -> b || (c == fst t))
-- /Resolución 1a

(?) :: Eq k => Dict k v -> k -> Bool

-- Resolución 1b : Operador infijo '?' para la función 'belongs'. Es igual a la función
	-- 'belongs' pero con el orden de los argumentos invertido.
(?) = flip belongs
-- /Resolución 1b

--Main> [("calle",[3]),("city",[2,1])] ? "city" 
--True

-- Ejercicio 2
get :: Eq k => k -> Dict k v -> v

-- Resolución 2a : Función 'get' implementada con foldr1. Como asumo que la clave pertenece,
	-- no hay problema con usar el último elemento como 'default'. Se le pasa la función
	-- auxiliar 'obtener' definida a continuación. Si la clave no pertenece al diccionario
	-- (lo cual no debería pasar por que asumo que sí), devuelve el último elemento. Si la
	-- clave está repetida (tampoco debería pasar porque se está representando un
	-- diccionario) devuelve la primera aparición. Si la lista es vacía, se rompe.
get c l = snd (foldr1 (obtener c) l)

	-- Función auxiliar 'obtener' utilizada para 'get'. Toma una clave 'c' del diccionario
	-- y devuelve una función que, dados dos elementos del diccionario, devuelve el primero
	-- si su clave coincide con la clave c o el segundo si no.
obtener :: Eq k => k -> ( (k,v) -> (k,v) -> (k,v) )
obtener = (\c -> \t1 t2 -> if c == fst t1 then t1 else t2)
-- /Resolución 2a

(!) :: Eq k => Dict k v -> k -> v

-- Resolución 2b : Operador infijo '!' para la función 'get'. Es igual a la función 'get'
	-- pero con el orden de los argumentos invertido.
(!) = flip get
-- /Resolución 2b

--Main> [("calle",[3]),("city",[2,1])] ! "city" 
--[2,1]

-- Ejercicio 3
insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v

-- Resolución 3 : Función 'insertWith'. Si la clave ya pertenece al diccionario, se
	-- utiliza la función map para recorrerlo y agregar la nueva definición. Se le pasa la
	-- función auxiliar 'aplicar' definida a continuación. Si la clave no pertenece al
	-- diccionario, se agrega una nueva tupla con la clave y la definición al final.
insertWith f c d l | l ? c = map (aplicar f c d) l
                   | otherwise = l ++ [(c,d)]

	-- Función auxiliar 'aplicar' utilizada para 'insertWith'. Toma una función 'f', una
		-- clave 'c' y una definición 'd' y devuelve una función que, dada una tupla devuelve
		-- otra tupla. Si la clave 'c' coincide con la clave de la tupla, se le aplica la
		-- función 'f' a la definición de la tupla devuelta y la definición 'd'. Si la clave
		-- 'c' es distinta a la clave de la tupla, la tupla devuelta es la misma que la
		-- recibida.
aplicar :: Eq k => (v -> v -> v) -> k -> v -> ( (k,v) -> (k,v) )
aplicar f c d = (\t -> if c == fst t then ( c, f (snd t) d ) else t)
-- /Resolución 3

--Main> insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))
--[(1,"lab"),(2,"p")]

-- Ejercicio 4
groupByKey :: Eq k => [(k,v)] -> Dict k [v]

-- Resolución 4 : Función 'groupByKey' implementada con foldl. Se le pasa la función auxiliar
	-- 'agrupar' (con orden de argumentos invertido) definida a continuación y la lista vacía
	-- como caso base.
groupByKey = foldl (flip agrupar) []

	-- Función auxiliar 'agrupar' utilizada para 'groupByKey'. Esta a su vez utiliza la
	-- función 'insertWith' del ejercicio 3 para agregar la definición (segundo elemento
	-- de la tupla 't') correspondiente a la clave (primer elemento de la tupla 't') en el
	-- diccionario. A 'insertWith' se le pasa la función (++) para que, si la clave ya
	-- pertenece al diccionario, la definición se concatene a las demás definiciones.
agrupar :: Eq k => ((k,v) -> [(k,[v])] -> [(k,[v])])
agrupar t = insertWith (++) (fst t) [(snd t)]
-- /Resolución 4

-- Ejercicio 5
unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v

-- Resolución 5 : Función 'unionWith' implementada con foldr. Se le pasa la función auxiliar
	-- 'unir' definida a continuación y el primer diccionario como caso base.
unionWith f d = foldr (unir f) d

	-- Función auxiliar 'unir' utilizada para 'unionWith'. Toma una función 'f' y devuelve
		-- una función que, dado un elemento de un diccionario lo agrega a otro diccionario
		-- con 'insertWith'. A esta se le pasa la función 'f', y las componentes de la tupla,
		-- es decir, la clave y la definición.
unir :: Eq k => (v -> v -> v) -> ((k,v) -> Dict k v -> Dict k v)
unir f = (\t -> insertWith f (fst t) (snd t))
-- /Resolución 5

--Main> unionWith (++) [("calle",[3]),("city",[2,1])] [("calle", [4]), ("altura", [1,3,2])]
--[("calle",[3,4]),("city",[2,1]),("altura",[1,3,2])]


-- ------------------------------Sección 2--------------MapReduce---------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]

-- Ejercicio 6
distributionProcess :: Int -> [a] -> [[a]]

-- Resolución 6 : Función 'distributionProcess' implementada con foldr. Se le pasa la función
	-- auxiliar 'distribuir' definida a continuación y una lista de listas vacías como caso
	-- base. Falla si el número pasado como argumento es menor o igual a cero.
distributionProcess n = foldr distribuir (replicate n [])

	-- Función auxiliar 'distribuir' utilizada para 'distributionProcess'. Toma un elemento
	-- y una lista de listas de elementos y devuelve la misma lista de listas pero
	-- agregándole el elemento a la primer lista. Para asegurar balanceo (esta función será
	-- utilizada por 'distributionProcess' una vez por cada elemento de la lista original) la
	-- lista a la que se le agregó el elemento se pasa al final de la lista de listas.
distribuir :: a -> [[a]] -> [[a]]
distribuir = (\x l -> (tail l) ++ [x:(head l)])
-- /Resolución 6

-- Ejercicio 7
mapperProcess :: Eq k => Mapper a k v -> [a] -> [(k,[v])]

-- Resolución 7 : Función 'mapperProcess'. Primero utiliza la función 'map' para aplicar
	-- la función de mapeo a la lista de elementos ( [a] -> [[(k,v)]] ). Luego utiliza la
	-- función 'concat' para reorganizar la lista de listas de tuplas en una lista de
	-- tuplas ( [[(k,v)]] -> [(k,v)] ). Finalmente, agrupa esta lista por claves, es decir,
	-- genera un diccionario (una lista de tuplas sin claves repetidas) mediante la función
	-- 'groupByKey' ( [(k,v)] -> Dict k [v] ). NO TESTEADA!
mapperProcess m l = groupByKey (concat (map m l))
-- /Resolución 7

-- Ejercicio 8
combinerProcess :: (Eq k, Ord k) => [[(k, [v])]] -> [(k,[v])]

-- Resolución 8 : Función 'combinerProcess'. Primero utiliza la función 'foldr1' para
	-- combinar todos los diccionarios de la lista de diccionarios en uno solo
	-- ( [Dict k [v]] -> Dict k [v] ). Se le pasa la función 'unionWith' que combina
	-- dos diccionarios. Luego se ordenan los elementos del diccionario con la función
	-- 'sortBy'. Se le pasa como función de ordenamiento la función auxiliar 'ordenK'
	-- definida a continuación.
combinerProcess l = sortBy (ordenK) (foldr1 (unionWith (++)) l)

	-- Función auxiliar 'ordenK' utilizada como argumento de 'sortBy' en 'combinerProcess'.
	-- Es una función de ordenamiento que, dadas dos tuplas, las ordena según su clave.
ordenK :: Ord k => (k,[v]) -> (k,[v]) -> Ordering
ordenK a b | (fst a) < (fst b) = LT
           | otherwise = GT
-- /Resolución 8

--Main> combinerProcess [[(1,["Uno","uno"]),(2,["Dos","dos"]),(5,["Cinco","cinco"])],[(1,["One","one"]),(2,["Two","two"]),(4,["Four","four"]),(6,["Six","six"])],[(2,["II"]),(3,["III"]),(4,["IV"]),(5,["V"]),(6,["VI"])],[(1,["1"]),(3,["3"]),(6,["6"])]]
--[(1,["Uno","uno","One","one","1"]),(2,["Dos","dos","Two","two","II"]),(3,["III","3"]),(4,["Four","four","IV"]),(5,["Cinco","cinco","V"]),(6,["Six","six","VI","6"])]

-- Ejercicio 9
reducerProcess :: Reducer k v b -> [(k, [v])] -> [b]

-- Resolución 9 : Función 'reducerProcess'. Primero utiliza la función 'map' para aplicar
	-- la función de reducción a la lista de elementos ( [(k,[v])] -> [[b]] ). Luego utiliza
	-- la función 'concat' para aplanar la lista de listas en una lista ( [[b]] -> [b] ). NO
	-- TESTEADA!
reducerProcess r d = concat (map r d)
-- /Resolución 9

-- Ejercicio 10
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]

-- Resolución 10 : Función 'mapReduce'. Aplica cada una de las funciones definidas
	-- anteriormente. Utiliza la función 'map' para simular el procesamiento distribuído.
mapReduce m r l = reducerProcess r (combinerProcess (map (mapperProcess m) (distributionProcess 100 l)))
-- /Resolución 10

-- Ejercicio 11
visitasPorMonumento :: [String] -> Dict String Int

-- Resolución 11 : Función 'visitasPorMonumento' ...Aplica la función 'mapper1' que crea
	--tuplas (monumento, unidad) para que luego sume todas las veces que se visito al monumento(sumando todas las unidades).
visitasPorMonumento = mapReduce mapper1 reducer1

	-- Función auxiliar 'mapper1' ...
mapper1 :: Mapper String String Int
mapper1 = (\x -> (x,1):[])

	-- Función auxiliar 'reducer1' ...
reducer1 :: Reducer String Int (String,Int)
reducer1 = (\x -> (fst x, foldr1 (+) (snd x)):[])
-- /Resolución 11

-- Ejercicio 12
monumentosTop :: [String] -> [String]

-- Resolución 12 : Función 'monumentosTop' ...
monumentosTop l = secondIt (firstIt l)

	-- Función auxiliar firstIt ...
firstIt :: [String] -> Dict Int String
firstIt = mapReduce mapper2first reducer2first

	-- Función auxiliar secondIt ...
secondIt :: Dict Int String -> [String]
secondIt = mapReduce mapper2second reducer2second

	-- Función auxiliar 'mapper2first' ...
mapper2first :: Mapper String String Int
mapper2first = (\x -> (x,-1):[])

	-- Función auxiliar 'reducer2first' ...
reducer2first :: Reducer String Int (Int, String)
reducer2first = (\x -> (foldr1 (+) (snd x), fst x):[])

	-- Función auxiliar 'mapper2second' ...
mapper2second :: Mapper (Int,String) Int String
mapper2second = (\x -> x:[])

	-- Función auxiliar 'reducer2second' ...
reducer2second :: Reducer Int String String
reducer2second = (\x -> (snd x))
-- /Resolución 12

-- Ejercicio 13 
monumentosPorPais :: [(Structure, Dict String String)] -> [(String, Int)]

-- Resolución 13 : Función 'monumentosPorPais' ...
monumentosPorPais = mapReduce mapper3 reducer3

	-- Función auxiliar 'mapper3' ...
mapper3 :: Mapper (Structure, Dict String String) String Int
mapper3 (Monument, x) = ((x ! "country"), 1):[]
mapper3 _ = []

	-- Función auxiliar 'reducer3' ...
reducer3 :: Reducer String Int (String,Int)
reducer3 = (\x -> (fst x, foldr1 (+) (snd x)):[])
-- /Resolución 13

-- ------------------------ Ejemplo de datos del ejercicio 13 ----------------------
data Structure = Street | City | Monument deriving Show

items :: [(Structure, Dict String String)]
items = [
    (Monument, [
      ("name","Obelisco"),
      ("latlong","-36.6033,-57.3817"),
      ("country", "Argentina")]),
    (Street, [
      ("name","Int. Güiraldes"),
      ("latlong","-34.5454,-58.4386"),
      ("country", "Argentina")]),
    (Monument, [
      ("name", "San Martín"),
      ("country", "Argentina"),
      ("latlong", "-34.6033,-58.3817")]),
    (City, [
      ("name", "Paris"),
      ("country", "Francia"),
      ("latlong", "-24.6033,-18.3817")]),
    (Monument, [
      ("name", "Bagdad Bridge"),
      ("country", "Irak"),
      ("new_field", "new"),
      ("latlong", "-11.6033,-12.3817")])
    ]


------------------------------------------------
------------------------------------------------
