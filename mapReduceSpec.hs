--  Para correr los tests deben cargar en hugs el módulo Tests
--  y evaluar la expresión "main".
-- Algunas funciones que pueden utilizar para chequear resultados:
-- http://hackage.haskell.org/package/hspec-expectations-0.6.1/docs/Test-Hspec-Expectations.html#t:Expectation

import Test.Hspec
import MapReduce
import Data.Ord
import Data.List

main :: IO ()
main = hspec $ do
  describe "Utilizando Diccionarios" $ do
    it "puede determinarse si un elemento es una clave o no" $ do
      -- Ej1 a
      belongs 3 [(3, "A"), (0, "R"), (7, "G")]                                  `shouldBe` True
      belongs "k" []                                                            `shouldBe` False
      belongs [] [(["El","Los"],55),([],6),(["La","Las"],122)]	                `shouldBe` True
      belongs 4 [(1,["Uno","One","I"]),(3,["Tres","III"]),(5,["Five","V"])]     `shouldBe` False
      -- Ej1 b
      [("H", [1]), ("E", [2]), ("Y", [0])] ? "R"                                `shouldBe` False
      [("V", [1]), ("O", [2]), ("S", [0])] ? "V"                                `shouldBe` True
      [("calle",[3]),("city",[2,1])] ? "city"                                   `shouldBe` True
      [(94.3,"Disney"),(790,"Mitre"),(99.9,"La cien"),(95.9,"Rock&Pop")] ? 99.9 `shouldBe` True
      [([1,3],"Dos"),([3,2,1],"Reverse")] ? [1,2,3]                             `shouldBe` False

    it "puede obtenerse la definicón de una clave siempre que esté definida" $ do
      -- Ej2 a
      get 6 [(2,8),(6,4),(3,7),(5,5)]                                           `shouldBe` 4
      get "Rey" [("Torre",[0,7]),("Alfil",[2,5]),("Rey",[3]),("Reina",[4])]     `shouldBe` [3]
      -- Ej2 b
      [("calle",[3]),("city",[2,1])] ! "city"                                   `shouldBe` [2,1]
      [("calle","San Blas"),("city","Hurlingham")] ! "city"                     `shouldBe` "Hurlingham"
      superpoderes ! "Aquaman"                                                  `shouldBe` ["Nada"]
      finales ! "Análisis"                                                      `shouldBe` False

    it "pueden agregarse definiciones sin repetir claves" $ do
      -- Ej3
      insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))     `shouldMatchList` [(1,"lab"),(2,"p")]
      insertWith (++) 1 [99] [(1 , [1]) , (2 , [2])] ! 1                                     `shouldMatchList` [1,99]
      insertWith (++) 3 [99] [(1 , [1]) , (2 , [2])]                                         `shouldMatchList` [(1 ,[1]) ,(2 ,[2]) ,(3 ,[99])]
      insertWith (+) "Alemania" 4 (insertWith (-) "Brasil" 3 [("Alemania",3),("Brasil",4)])  `shouldMatchList` [("Alemania",7),("Brasil",1)]
      insertWith (+) "Argentina" 0 [("Alemania",1)]                                          `shouldMatchList` [("Argentina",0),("Alemania",1)]
      insertWith (&&) ["AR$","U$S"] False cambio ! ["AR$","U$S"]                             `shouldBe` False
      insertWith (&&) ["U$S","AR$"] True cambio ! ["U$S","AR$"]                              `shouldBe` False

    it "puede generarse un diccionario a partir de una lista de tuplas" $ do
      -- Ej4
      groupByKey calles ! "calle"              `shouldMatchList` ["Jean Jaures","7"]
      groupByKey calles ! "ciudad"             `shouldMatchList` ["Brujas","Kyoto"]
      groupByKey divisores ! 4                 `shouldMatchList` [2]
      groupByKey divisores ! 6                 `shouldMatchList` [2,3]
      groupByKey divisores ! 8                 `shouldMatchList` [2,4]
      groupByKey divisores ! 9                 `shouldMatchList` [3]
      groupByKey divisores ! 10                `shouldMatchList` [2,5]
      groupByKey divisores ! 12                `shouldMatchList` [2,3,4,6]
      groupByKey [(0,1),(1,0)]                 `shouldMatchList` [(0,[1]),(1,[0])]
      groupByKey listaVacia                    `shouldBe` dictVacio

    it "pueden combinarse dos diccionarios en uno" $ do
      -- Ej5
      unionWith (++) callesA callesB ! "calle"                                            `shouldMatchList` ["3","4"]
      unionWith (++) callesA callesB ! "ciudad"                                           `shouldMatchList` ["1","2"]
      unionWith (++) callesA callesB ! "altura"                                           `shouldMatchList` ["1","2","3"]
      unionWith (+) rutasA rutasB ! "rutas"                                               `shouldBe` 7
      unionWith (+) rutasA rutasB ! "ciclos"                                              `shouldBe` 1
      unionWith (++) (groupByKey calles) (unionWith (++) callesA callesB) ! "calle"       `shouldMatchList` ["3","4","Jean Jaures","7"]
      unionWith (++) (groupByKey calles) (unionWith (++) callesA callesB) ! "ciudad"      `shouldMatchList` ["1","2","Brujas","Kyoto"]
      unionWith (++) (groupByKey calles) (unionWith (++) callesA callesB) ! "altura"      `shouldMatchList` ["1","2","3"]
      unionWith (++) dictVacio callesA                                                    `shouldMatchList` callesA
      unionWith (++) callesB dictVacio                                                    `shouldMatchList` callesB

  describe "Utilizando funciones de distribución y combinación" $ do
    it "puede distribuírse un conjunto en n subconjuntos balanceados" $ do
      -- Ej6
      distributionProcess 1 divisores                `shouldMatchList` [divisores]
      distributionProcess 1 primeros12               `shouldMatchList` [primeros12]
      length (distributionProcess 5 primeros12)      `shouldBe` 5
      concat (distributionProcess 5 primeros12)      `shouldMatchList` primeros12
      distributionProcess 5 primeros12               `shouldSatisfy` balanceo
      length (distributionProcess 7 divisores)       `shouldBe` 7
      concat (distributionProcess 7 divisores)       `shouldMatchList` divisores
      distributionProcess 7 divisores                `shouldSatisfy` balanceo

    it "puede aplicarse la funcion de mapeo a cada elemento" $ do
      -- Ej7
      mapperProcess (\x -> [(x,1)]) paises                                                `shouldMatchList` dictPaises
      mapperProcess (\x -> [(p, fst x) | p <- snd x]) superpoderes ! "Fuerte"             `shouldMatchList` ["Superman"]
      mapperProcess (\x -> [(p, fst x) | p <- snd x]) superpoderes ! "Rápido"             `shouldMatchList` ["Superman","Flash"]
      mapperProcess (\x -> [(p, fst x) | p <- snd x]) superpoderes ! "Visión Láser"       `shouldMatchList` ["Superman"]
      mapperProcess (\x -> [(p, fst x) | p <- snd x]) superpoderes ! "Vuela"              `shouldMatchList` ["Superman"]
      mapperProcess (\x -> [(p, fst x) | p <- snd x]) superpoderes ! "Nada"               `shouldMatchList` ["Aquaman"]
      mapperProcess (\x -> [("1","1")]) listaVacia                                        `shouldBe` dictVacio

    it "pueden combinarse los resultados de distintos procesos" $ do
      -- Ej8
      combinerProcess (distributionProcess 3 superpoderes)       `shouldMatchList` superpoderes
      combinerProcess (distributionProcess 4 dictPaises)         `shouldMatchList` dictPaises
      combinerProcess numeros ! 1                                `shouldMatchList` numerosCombinados ! 1
      combinerProcess numeros ! 2                                `shouldMatchList` numerosCombinados ! 2
      combinerProcess numeros ! 3                                `shouldMatchList` numerosCombinados ! 3
      combinerProcess numeros ! 4                                `shouldMatchList` numerosCombinados ! 4
      combinerProcess numeros ! 5                                `shouldMatchList` numerosCombinados ! 5
      combinerProcess numeros ! 6                                `shouldMatchList` numerosCombinados ! 6
      combinerProcess [dictVacio]                                `shouldBe` dictVacio

    it "puede aplicarse la función de reducción a cada elemento" $ do
      -- Ej9
      reducerProcess (\x -> [fst x]) numerosCombinados           `shouldMatchList` [1,2,3,4,5,6]
      reducerProcess reducerHeroes superpoderes                  `shouldMatchList` heroes

    it "puede aplicarse el método 'mapReduce' completo" $ do
      -- Ej10
      mapReduce (\x -> [(snd x, fst x)]) (\x -> [(a, (fst x)) | a <- (snd x)]) finales    `shouldMatchList` finales
      mapReduce mapDivisores reduceDivisores primeros12 ! 1                               `shouldMatchList` []
      mapReduce mapDivisores reduceDivisores primeros12 ! 2                               `shouldMatchList` []
      mapReduce mapDivisores reduceDivisores primeros12 ! 3                               `shouldMatchList` []
      mapReduce mapDivisores reduceDivisores primeros12 ! 4                               `shouldMatchList` [2]
      mapReduce mapDivisores reduceDivisores primeros12 ! 5                               `shouldMatchList` []
      mapReduce mapDivisores reduceDivisores primeros12 ! 6                               `shouldMatchList` [2,3]
      mapReduce mapDivisores reduceDivisores primeros12 ! 7                               `shouldMatchList` []
      mapReduce mapDivisores reduceDivisores primeros12 ! 8                               `shouldMatchList` [2,4]
      mapReduce mapDivisores reduceDivisores primeros12 ! 9                               `shouldMatchList` [3]
      mapReduce mapDivisores reduceDivisores primeros12 ! 10                              `shouldMatchList` [2,5]
      mapReduce mapDivisores reduceDivisores primeros12 ! 11                              `shouldMatchList` []
      mapReduce mapDivisores reduceDivisores primeros12 ! 12                              `shouldMatchList` [2,3,4,6]

  describe "Utilizando Map Reduce" $ do
    it "visitas por monumento funciona en algún orden" $ do
      -- Ej11
      visitasPorMonumento ["m1", "m2", "m3", "m2", "m1", "m3", "m3"]  `shouldMatchList` [("m3",3), ("m1",2), ("m2",2)]
      visitasPorMonumento paises                                      `shouldMatchList` map (\x -> (fst x, foldr1 (+) (snd x))) dictPaises

    it "monumentosTop devuelve los más visitados en algún orden" $ do 
      -- Ej12
      monumentosTop ["m1", "m0", "m0", "m0", "m2", "m2", "m3"]        `shouldSatisfy` (\res -> res == ["m0", "m2", "m3", "m1"] || res == ["m0", "m2", "m1", "m3"])
      monumentosTop paises !! 0                                       `shouldBe` "Argentina"
      monumentosTop paises                                            `shouldSatisfy` (\r -> (r !! 1 == "Brasil" && r !! 2 == "Japon") || (r !! 1 == "Japon" && r !! 2 == "Brasil"))
      drop 3 (monumentosTop paises)                                   `shouldMatchList` ["Uruguay","Alemania","Australia"]

    it "monumentosPorPais" $ do
      -- Ej13
      monumentosPorPais items                                         `shouldMatchList` [("Argentina",2),("Irak",1)]

superpoderes :: Dict String [String]
superpoderes =  [("Superman",["Fuerte","Rápido","Vuela","Visión Láser"]),("Aquaman",["Nada"]),("Green Arrow",["-"]),("Flash",["Rápido"]),("Batman",["-"])]

finales :: Dict String Bool
finales = [("Análisis",False),("Algebra",True),("AlgoI",True),("OrgaI",True),("AlgoII",True),("AlgoIII",True),("OrgaII",True),("SO",True),("IS1",True)]

cambio :: Dict [String] Bool
cambio = [(["EUR","U$S"],True),(["AR$","EUR"],True),(["U$S","AR$"],False),(["AR$","U$S"],True),(["EUR","AR$","U$S"],True),(["AR$","EUR","AR$"],False)]

calles :: [(String,String)]
calles = [("calle","Jean Jaures"),("ciudad","Brujas"),("ciudad","Kyoto"),("calle","7")]

listaVacia :: [(String, String)]
listaVacia = []

dictVacio :: Dict String [String]
dictVacio = []

callesA :: Dict String [String]
callesA = [("calle",["3"]),("ciudad",["2","1"])]

callesB :: Dict String [String]
callesB = [("calle", ["4"]), ("altura", ["1","3","2"])]

rutasA :: Dict String Int
rutasA = [("rutas",3)]

rutasB :: Dict String Int
rutasB = [("rutas",4),("ciclos",1)]

divisores :: [(Int,Int)]
divisores = [(6,3),(12,4),(8,2),(10,5),(12,6),(8,4),(9,3),(4,2),(12,3),(6,2),(10,2),(12,2)]

primeros12 :: [Int]
primeros12 = [1,2,3,4,5,6,7,8,9,10,11,12]

balanceo :: [[a]] -> Bool
balanceo = (\res -> length (maximumBy (comparing length) res) <= 1 + length (minimumBy (comparing length) res))

paises :: [String]
paises = ["Argentina","Brasil","Alemania","Argentina","Argentina","Brasil","Uruguay","Japon","Australia","Japon","Argentina"]

dictPaises :: Dict String [Int]
dictPaises = [("Argentina",[1,1,1,1]),("Brasil",[1,1]),("Alemania",[1]),("Uruguay",[1]),("Japon",[1,1]),("Australia",[1])]

numeros :: [Dict Int [String]]
numeros = [[(1,["Uno","uno"]),(2,["Dos","dos"]),(5,["Cinco","cinco"])],[(1,["One","one"]),(2,["Two","two"]),(4,["Four","four"]),(6,["Six","six"])],[(2,["II"]),(3,["III"]),(4,["IV"]),(5,["V"]),(6,["VI"])],[(1,["1"]),(3,["3"]),(6,["6"])]]

numerosCombinados :: Dict Int [String]
numerosCombinados = [(1,["Uno","uno","One","one","1"]),(2,["Dos","dos","Two","two","II"]),(3,["III","3"]),(4,["Four","four","IV"]),(5,["Cinco","cinco","V"]),(6,["Six","six","VI","6"])]

reducerHeroes :: Reducer String String String
reducerHeroes = (\x -> [fst x ++ ": " ++ (foldr1 (\x y -> x ++ ", " ++ y) (snd x))])

heroes :: [String]
heroes = [("Batman: -"),("Green Arrow: -"),("Aquaman: Nada"),("Superman: Fuerte, Rápido, Vuela, Visión Láser"),("Flash: Rápido")]

mapDivisores :: Mapper Int Int [Int]
mapDivisores = (\x -> [(x, [(snd d) | d <- divisores, fst d == x])])

reduceDivisores :: Reducer Int [Int] (Int, [Int])
reduceDivisores = (\x -> [(fst x, (concat (snd x)))])
