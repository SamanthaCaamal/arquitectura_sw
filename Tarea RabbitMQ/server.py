

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')

def fib(n):
    f = open("archivos/indices.txt")

    for linea in f:
        caracteres = linea.split(' ')
        print (linea)
        if caracteres[0] == str(n)+',':
            resultfile = open("archivos/" + caracteres[1].rstrip('\n'))
            return resultfile.read()


def on_request(ch, method, props, body):
    n = int(body)
    response = fib(n)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='rpc_queue')

print(" [x] Awaiting RPC requests")
channel.start_consuming()