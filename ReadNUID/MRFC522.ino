#include <SPI.h>
#include <MFRC522.h>

#define SS_PIN 5
#define RST_PIN 0

MFRC522 rfid(SS_PIN, RST_PIN);

void SetupRFID() {
  SPI.begin();         // Inicialitza el bus SPI
  rfid.PCD_Init();     // Inicialitza el lector RFID
  Serial.println("RFID inicialitzat correctament.");
}

bool CheckRFID(String &tag) {
  // Si no hi ha cap targeta nova, no fem res
  if (!rfid.PICC_IsNewCardPresent() || !rfid.PICC_ReadCardSerial()) {
    return false;
  }

  // Converteix el UID a String hexadecimal
  tag = "";
  for (byte i = 0; i < rfid.uid.size; i++) {
    if (rfid.uid.uidByte[i] < 0x10)
      tag += "0";
    tag += String(rfid.uid.uidByte[i], HEX);
  }

  tag.toUpperCase(); 

  // Atura la comunicació amb la tarjeta
  rfid.PICC_HaltA();
  rfid.PCD_StopCrypto1();

  Serial.println("Lectura completada correctament.");
  return true; // Hay tarjeta
}
