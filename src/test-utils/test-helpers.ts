/**
 * Test utility functions
 */

/**
 * Deep freezes an object recursively, ensuring all nested objects and arrays are frozen
 * @param obj - The object to deep freeze
 * @returns The deep frozen object
 */
export const deepFreeze = (obj: any): any => {
  Object.freeze(obj);
  Object.getOwnPropertyNames(obj).forEach(prop => {
    if (obj[prop] !== null && typeof obj[prop] === 'object' && !Object.isFrozen(obj[prop])) {
      deepFreeze(obj[prop]);
    }
  });
  return obj;
};
