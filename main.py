import csv
import paho.mqtt.client as mqtt
import data_manager

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("tnt")
    arquivo = open('dados.csv', 'w', newline='', encoding='utf-8')
    arquivo.write("Tempo,Estação,LAT,LONG,Movimentação,Original_473,Original_269,Zero,Maçã-Verde,Tangerina,Citrus,Açaí-Guaraná,Pêssego,TARGET,row\n")
    arquivo.close()


def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload, 'utf-8'))
    dados_coletados = str(msg.payload, 'utf-8')
    dados_coletados = dados_coletados.split(',')

    resultado = []

    for dado in dados_coletados:
        dado = dado.split(':')
        resultado.append(dado[1])

    resultado = str(resultado)
    resultado = resultado.replace('\"', '')
    resultado = resultado.replace('\'', '')
    resultado = resultado.replace('}', '')
    resultado = resultado.replace(' ', '')
    resultado = resultado.replace('[', '')
    resultado = resultado.replace(']', '')
    resultado = resultado.split(',')

    verificacao = data_manager.confere_registros(resultado[14])

    if verificacao[0] == 'Novo registro':
        with open('dados.csv', 'a', newline='', encoding='utf-8') as resultados:
            gravacao = csv.writer(resultados)
            gravacao.writerow(resultado)
        print('Status: {} |=> Registros: {}'.format(verificacao[0],verificacao[1]+1))
    else:
        print('Status: {} |=> Registros: {}'.format(verificacao[0],verificacao[1]))

    if int(verificacao[1]) == 17016:
        print('Terminou!!!')
        client.disconnect()

if __name__ == '__main__':
    client = mqtt.Client()
    client.username_pw_set('maratoners',"ndsjknvkdnvjsbvj")
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect("tnt-iot.maratona.dev", 30573)
    client.loop_forever()