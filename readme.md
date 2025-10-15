# docker SciKey 

ejecutar el siguiente comando en la terminal desde la carpeta donde se encuentra el archivo docker-compose.yml


debes correr este comando solo la primera vez para crear la imagen y el contenedor

debe esta en donde se encuentra el archivo docker-compose.yml
```sh
$ ./docker-compose up -d 


docker compose down --rmi all --volumes 
docker rm -f mysql-container-scikey && docker rmi scikey-mysql-db
```