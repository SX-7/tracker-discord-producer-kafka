# tracker-discord-producer-kafka
Contains the producer, consumer is at https://github.com/SX-7/tracker-consumer-kafka

Each should be ran seperately, and it is not neccesary to have one *and* the other online at the same time

### Remember to put .env file in the same folder as producer.py
### Ensure that you have provided identical kafka credentials for both consumer and producer

Zadania:
1. Stwórz aplikację która połączy się z Discordem za pomocą przydzielonego Tobie tokena. Kiedy nowy użytkownik dołączy do serwera, aplikacja powinna wypisać w konsoli nazwę użytkownika. Dokumentacja discord.py: https://discordpy.readthedocs.io/en/stable/api.html
2. Do programu z poprzedniego punktu dodaj funkcjonalność wypisywania poprzedniej i obecnej nazwy użytkownika oraz kiedy ten użytkownik zmienił nazwę.
3. Spraw by aplikacja zamiast wyświetlać dane z poprzednich punktów w konsoli, działała jako producent, i wysyłała je na serwer Kafki. 
Dokumentacja kafka-python: https://kafka-python.readthedocs.io/en/master/index.html
4. Stwórz konsumenta łączącego się z serwerem Kafki, i odczytującego wiadomości z tego serwera. Zapewnij by działał dla obydwóch zdefiniowanych przez Ciebie w poprzednich punktach rodzajów wiadomości.
