export function convertLatToDMM(decimalDegrees: number): string {
  const abs = Math.abs(decimalDegrees);
  const degrees = Math.floor(abs);
  const minutes = ((abs - degrees) * 60).toFixed(3);
  const direction = decimalDegrees >= 0 ? "N" : "S";

  return `${degrees.toString().padStart(2, "0")}${minutes
    .replace(".", "")
    .padStart(5, "0")}${direction}`;
}

export function convertLonToDMM(decimalDegrees: number): string {
  const abs = Math.abs(decimalDegrees);
  const degrees = Math.floor(abs);
  const minutes = ((abs - degrees) * 60).toFixed(3);
  const direction = decimalDegrees >= 0 ? "E" : "W";

  return `${degrees.toString().padStart(3, "0")}${minutes.replace(
    ".",
    ""
  )}${direction}`;
}
