#Jogo de adivinhar numeros

import random

idade = round(random.randrange(1,101))
chances = 5

for rodada in range(1,chances+1):
    print('Tentativa {} de {}'.format(rodada, chances))
    chute_str = input('Quantos anos eu tenho? \n')
    chute = int(chute_str)

    print('Você acha que eu tenho ',chute, 'anos ?', end='\n')

    if(chute < 10 or chute > 35):
        print('Chute uma idade coerente')
        continue

    acerto = chute == idade
    maior = chute > idade
    menor = chute < idade

    if(acerto):
        print('certo!')
        break
    else:
        if(maior):
            print('Sou mais novo!')
        elif(menor):
            print('Sou mais veho!')

    rodada = rodada +1

print('Fim do jogo')