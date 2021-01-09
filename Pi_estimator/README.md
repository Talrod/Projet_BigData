# Estimation de Pi

Ce projet a pour objectif d'estimer pi en utilisant Spark et Numpy.

## Instructions

:arrow_forward: Ouverture de l'invite de commande

:arrow_forward: Rentrer la commande : 

     "spark-submit Pi.py"

:arrow_forward: Après l'execution du programme, les estimations de pi caclulées avec la méthode de Spark et Numpy seront affichées directement dans la console. Il y aura également le temps d'execution de chacune des ces méthodes :

![](https://github.com/Talrod/Projet_BigData/blob/main/Pi_estimator/Output/Output.PNG)

## Comparaison des résultats

| n = 100000 | Spark | Numpy |
|------------|------:|------:|
|Temps d'éxécution (secondes)| 8.9586 | 0.0404 |
|Valeur de Pi | 3.150800 | 3.138240 |
|Écart % Math.pi | 0.29% |  -0.11% | 

:heavy_check_mark: On constate que Spark n'est pas plus performant que Numpy. En effet, le temps d'execution est plus important sans pour autant obtenir une plus grande précision.
Lorsque l'on execute le programme avec n = 1 000 000, le même constat est fait. Cependant, on observe que le temps d'execution de numpy augmente légérement.

:heavy_exclamation_mark: Toutefois, lorsque l'on execute le programme avec n = 100 000 000, Spark devient la méthode la plus rapide. 

