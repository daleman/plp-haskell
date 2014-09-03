--  Para correr los tests deben cargar en hugs el módulo Tests
--  y evaluar la expresión "main".
-- Algunas funciones que pueden utilizar para chequear resultados:
-- http://hackage.haskell.org/package/hspec-expectations-0.6.1/docs/Test-Hspec-Expectations.html#t:Expectation

import Test.Hspec
import MapReduce

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
      [("Superman",["Fuerte","Rápido","Vuela","Visión Láser"]),("Aquaman",["Nada"]),("Green Arrow",[]),("Flash",["Rápido"]),("Batman",[])] ! "Aquaman"            `shouldBe` ["Nada"]
      [("Análisis",False),("Algebra",True),("AlgoI",True),("OrgaI",True),("AlgoII",True),("AlgoIII",True),("OrgaII",True),("SO",True),("IS1",True)] ! "Análisis"  `shouldBe` False

    it "pueden agregarse definiciones sin repetir claves" $ do
      -- Ej3
      insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))      `shouldMatchList` [(1,"lab"),(2,"p")]
      insertWith (++) 1 [99] [(1 , [1]) , (2 , [2])] ! 1                                      `shouldMatchList` [1,99]
      insertWith (++) 3 [99] [(1 , [1]) , (2 , [2])]                                          `shouldMatchList` [(1 ,[1]) ,(2 ,[2]) ,(3 ,[99])]
      insertWith (+) "Alemania" 4 (insertWith (-) "Brasil" 3 [("Alemania",3),("Brasil",4)])   `shouldMatchList` [("Alemania",7),("Brasil",1)]
      insertWith (+) "Argentina" 0 [("Alemania",1)]                                           `shouldMatchList` [("Argentina",0),("Alemania",1)]
      insertWith (&&) ["AR$","U$S"] False [(["EUR","U$S"],True),(["U$S","AR$"],False),(["AR$","U$S"],True),(["AR$","EUR","AR$"],False)] ! ["AR$","U$S"]   `shouldBe` False
      insertWith (&&) ["U$S","AR$"] True [(["U$S","AR$"],False),(["U$S","EUR"],True),(["EUR","AR$"],True),(["AR$","EUR","AR$"],False)] ! ["U$S","AR$"]    `shouldBe` False

    --it "puede generarse un diccionario a partir de una lista de tuplas" $ do
      -- Ej4

  describe "Utilizando Map Reduce" $ do
    it "visitas por monumento funciona en algún orden" $ do
      visitasPorMonumento [ "m1" ,"m2" ,"m3" ,"m2","m1", "m3", "m3"] `shouldMatchList` [("m3",3), ("m1",2), ("m2",2)] 

    it "monumentosTop devuelve los más visitados en algún orden" $ do 
      monumentosTop [ "m1", "m0", "m0", "m0", "m2", "m2", "m3"] 
      `shouldSatisfy` (\res -> res == ["m0", "m2", "m3", "m1"] || res == ["m0", "m2", "m1", "m3"])
