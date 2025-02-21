# Bienvenue sur Ysearch Api

## Description
Ysearch Api est une application Django qui [expliquez brièvement ce que fait votre application, par exemple : "gère les utilisateurs, gère les inventaires, affiche des statistiques"]. Conçue pour être intuitive et rapide, cette application peut être facilement déployée et personnalisée.

---

## Prérequis
Avant d'utiliser cette application, assurez-vous d'avoir installé les éléments suivants :

1. **Dépendances** :
   - Installez les dépendances du fichier `requirements.txt` avec la commande :
     ```bash
     pip install -r requirements.txt
     ```

---

## Installation
1. Clonez ce dépôt :
   ```bash
   git clone https://github.com/votre-nom/votre-repo.git
   ```

2. Naviguez dans le répertoire du projet :
   ```bash
   cd votre-repo
   ```

3. Configurez la base de données :
   - Par défaut, SQLite est utilisé. Vous pouvez configurer d'autres bases dans `settings.py`.
   - Appliquez les migrations :
     ```bash
     python manage.py migrate
     ```

4. Lancez le serveur de développement :
   ```bash
   python manage.py runserver
   ```

5. Accédez à l'application via [http://127.0.0.1:8000/](http://127.0.0.1:8000/).

---

## Fonctionnalités principales
- **[CRUD Doamin name]** : Can create, read, update and Delete Domain name, and put it into a RabbitMQ queue
<!-- - **[Fonctionnalité 2]** : Description concise. -->
<!-- - **[Fonctionnalité 3]** : Description concise. -->

---

<!-- ## Configuration supplémentaire
### Fichier `.env` (si applicable)
Créez un fichier `.env` à la racine et ajoutez-y les clés suivantes :
```
DEBUG=True
SECRET_KEY=votre_cle_secrete
DATABASE_URL=votre_url_de_base_de_donnees
```
Utilisez la librairie `django-environ` pour charger ces variables dans votre projet. -->

### Administration Django
1. Créez un super utilisateur pour accéder au panneau d'administration :
   ```bash
   python manage.py createsuperuser
   ```
2. Connectez-vous au panneau admin via [http://127.0.0.1:8000/admin/](http://127.0.0.1:8000/admin/).

---

## Tests
1. Exécutez les tests unitaires :
   ```bash
   python manage.py test
   ```
2. Consultez les résultats des tests pour vérifier que tout fonctionne correctement.

---

## Support
Pour toute question ou problème, veuillez contacter :
<!-- - **Email** :  -->
- **Issues GitHub** : [https://github.com/Ysearch-motor/Ysearch-api/issues](https://github.com/Ysearch-motor/Ysearch-api/issues)

