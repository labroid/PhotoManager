from me_models import Db_connect, State

Db_connect()

State.objects().update_one(target=r"C:\Users\sjackson\Pictures\FZ80\moon")
print("Done")