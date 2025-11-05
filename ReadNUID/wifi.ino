#include <WiFi.h>

// Dades de la xarxa wifi
const char *ssid = "iPhone de Tefa";
const char *password = "chulin:33";

void SetupWifi() {
  Serial.println();
  Serial.print("[WiFi] Connectant a ");
  Serial.println(ssid);

  WiFi.begin(ssid, password);

  int intents = 0;
  int maxIntents = 20; // nombre màxim d’intents de connexió

  while (WiFi.status() != WL_CONNECTED && intents < maxIntents) {
    delay(500);
    Serial.print(".");
    intents++;
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("");
    Serial.println("Wi-Fi connectada correctament!");
    Serial.print("IP assignada: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("");
    Serial.println("No s'ha pogut connectar a la Wi-Fi!");
    Serial.println("Revisa l'SSID o la contrasenya.");
  }
}

void CheckWifi() {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[WiFi] Connexió perduda, tornant a connectar...");
    WiFi.disconnect();
    WiFi.begin(ssid, password);

    int intents = 0;
    while (WiFi.status() != WL_CONNECTED && intents < 20) {
      delay(500);
      Serial.print(".");
      intents++;
    }

    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("");
      Serial.println("Reconnectat correctament a la Wi-Fi!");
    } else {
      Serial.println("");
      Serial.println("No s'ha pogut reconnectar a la Wi-Fi.");
    }
  }
}