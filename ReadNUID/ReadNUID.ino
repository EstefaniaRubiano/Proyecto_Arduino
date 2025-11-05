#include <Arduino.h>

// Variables d'estat globals
bool WifiConnected = false;
bool AWSConnected = false;

// Declarem les funcions que estan en altres fitxers
void SetupRFID();
bool CheckRFID(String &tag);

void SetupWifi();
void CheckWifi();

void SetupAWS();
void CheckAWS();
void PublicaTag(String tag);

// Programa principal
// ─────────────────────────────────────────────────
void setup() {
  Serial.begin(9600);
  Serial.println("Iniciant sistema de lectura RFID...");

  // Inicialitza la connexió Wi-Fi
  SetupWifi();

  // Inicialitza el lector RFID
  SetupRFID();

  // Inicialitza la connexió amb AWS IoT Core
  SetupAWS();

  Serial.println("Preparat per llegir targetes");
  Serial.println("-----------------------------------");
}

void loop() {
  // Comprova que la connexió Wi-Fi segueixi activa
  CheckWifi();

  // Comprova i manté la connexió amb AWS IoT Core
  CheckAWS();

  // Comprova si s'ha llegit una targeta RFID
  String tag;
  if (CheckRFID(tag)) {
    Serial.print("Targeta detectada, UID: ");
    Serial.println(tag);

    // Si està connectat a AWS, publica la dada al núvol
    if (AWSConnected) {
      Serial.println("Enviant UID a AWS IoT...");
      PublicaTag(tag);
    } else {
      Serial.println("No connectat a AWS, no s'ha pogut publicar.");
    }

    Serial.println("-----------------------------------");
    delay(2000);
  }
}