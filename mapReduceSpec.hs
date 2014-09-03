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
      finales ! "Análisis"  `shouldBe` False

    it "pueden agregarse definiciones sin repetir claves" $ do
      -- Ej3
      insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))      `shouldMatchList` [(1,"lab"),(2,"p")]
      insertWith (++) 1 [99] [(1 , [1]) , (2 , [2])] ! 1                                      `shouldMatchList` [1,99]
      insertWith (++) 3 [99] [(1 , [1]) , (2 , [2])]                                          `shouldMatchList` [(1 ,[1]) ,(2 ,[2]) ,(3 ,[99])]
      insertWith (+) "Alemania" 4 (insertWith (-) "Brasil" 3 [("Alemania",3),("Brasil",4)])   `shouldMatchList` [("Alemania",7),("Brasil",1)]
      insertWith (+) "Argentina" 0 [("Alemania",1)]                                           `shouldMatchList` [("Argentina",0),("Alemania",1)]
      insertWith (&&) ["AR$","U$S"] False cambio ! ["AR$","U$S"]                              `shouldBe` False
      insertWith (&&) ["U$S","AR$"] True cambio ! ["U$S","AR$"]                               `shouldBe` False

    it "puede generarse un diccionario a partir de una lista de tuplas" $ do
      -- Ej4
      calles1 ! "calle"            `shouldMatchList` ["Jean Jaures","7"]
      calles1 ! "ciudad"           `shouldMatchList` ["Brujas","Kyoto"]
      groupByKey divisores ! 4     `shouldMatchList` [2]
      groupByKey divisores ! 6     `shouldMatchList` [2,3]
      groupByKey divisores ! 8     `shouldMatchList` [2,4]
      groupByKey divisores ! 9     `shouldMatchList` [3]
      groupByKey divisores ! 10    `shouldMatchList` [2,5]
      groupByKey divisores ! 12    `shouldMatchList` [2,3,4,6]
      groupByKey [(0,1),(1,0)]     `shouldMatchList` [(0,[1]),(1,[0])]

    it "pueden combinarse dos diccionarios en uno" $ do
      -- Ej5
      calles2 ! "calle"            `shouldMatchList` ["3","4"]
      calles2 ! "ciudad"           `shouldMatchList` ["1","2"]
      calles2 ! "altura"           `shouldMatchList` ["1","2","3"]
      rutas ! "rutas"              `shouldBe` 7
      rutas ! "ciclos"             `shouldBe` 1
      calles12 ! "calle"           `shouldMatchList` ["3","4","Jean Jaures","7"]
      calles12 ! "ciudad"          `shouldMatchList` ["1","2","Brujas","Kyoto"]
      calles12 ! "altura"          `shouldMatchList` ["1","2","3"]

  describe "Utilizando funciones de distribución y combinación" $ do
    it "puede distribuírse un conjunto en n subconjuntos balanceados" $ do
      -- Ej6
      length (distributionProcess 5 primeros12)      `shouldBe` 5
      distributionProcess 5 primeros12               `shouldSatisfy` balanceo
      length (distributionProcess 7 divisores)       `shouldBe` 7
      distributionProcess 7 divisores                `shouldSatisfy` balanceo

  describe "Utilizando Map Reduce" $ do
    it "visitas por monumento funciona en algún orden" $ do
      visitasPorMonumento [ "m1" ,"m2" ,"m3" ,"m2","m1", "m3", "m3"] `shouldMatchList` [("m3",3), ("m1",2), ("m2",2)] 

    it "monumentosTop devuelve los más visitados en algún orden" $ do 
      monumentosTop [ "m1", "m0", "m0", "m0", "m2", "m2", "m3"] 
      `shouldSatisfy` (\res -> res == ["m0", "m2", "m3", "m1"] || res == ["m0", "m2", "m1", "m3"])

superpoderes :: Dict String [String]
superpoderes =  [("Superman",["Fuerte","Rápido","Vuela","Visión Láser"]),("Aquaman",["Nada"]),("Green Arrow",[]),("Flash",["Rápido"]),("Batman",[])]

finales :: Dict String Bool
finales = [("Análisis",False),("Algebra",True),("AlgoI",True),("OrgaI",True),("AlgoII",True),("AlgoIII",True),("OrgaII",True),("SO",True),("IS1",True)]

cambio :: Dict [String] Bool
cambio = [(["EUR","U$S"],True),(["AR$","EUR"],True),(["U$S","AR$"],False),(["AR$","U$S"],True),(["EUR","AR$","U$S"],True),(["AR$","EUR","AR$"],False)]

calles1 :: [(String,[String])]
calles1 = groupByKey [("calle","Jean Jaures"),("ciudad","Brujas"),("ciudad","Kyoto"),("calle","7")]

divisores :: [(Int,Int)]
divisores = [(6,3),(12,4),(8,2),(10,5),(12,6),(8,4),(9,3),(4,2),(12,3),(6,2),(10,2),(12,2)]

calles2 :: Dict String [String]
calles2 = unionWith (++) [("calle",["3"]),("ciudad",["2","1"])] [("calle", ["4"]), ("altura", ["1","3","2"])]

rutas :: Dict String Int
rutas = unionWith (+) [("rutas",3)] [("rutas",4),("ciclos",1)]

calles12 :: Dict String [String]
calles12 = unionWith (++) calles1 calles2

primeros12 :: [Int]
primeros12 = [1,2,3,4,5,6,7,8,9,10,11,12]

balanceo :: [[a]] -> Bool
balanceo = (\res -> length (maximumBy (comparing length) res) <= 1 + length (minimumBy (comparing length) res))
