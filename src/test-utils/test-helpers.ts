/**
 * Test utility functions
 */

/**
 * Deep freezes an object recursively, ensuring all nested objects and arrays are frozen
 * @param obj - The object to deep freeze
 * @returns The deep frozen object
 */
export const deepFreeze = (obj: any): any => {
  // Return immediately for null, undefined, or non-objects
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }

  // Return immediately if already frozen
  if (Object.isFrozen(obj)) {
    return obj;
  }

  // Freeze the object
  Object.freeze(obj);

  // Recursively deep freeze own properties that are objects and not already frozen
  Object.getOwnPropertyNames(obj).forEach((prop) => {
    const propValue = obj[prop];
    if (propValue !== null && typeof propValue === 'object' && !Object.isFrozen(propValue)) {
      deepFreeze(propValue);
    }
  });

  return obj;
};
