import { User } from '@causa/runtime';
import { Auth, getAuth } from 'firebase-admin/auth';
import jwt from 'jsonwebtoken';
import * as uuid from 'uuid';
import { getDefaultFirebaseApp } from '../firebase/index.js';

/**
 * A helper to create and delete Identity Platform users in the emulator.
 * It also creates an ID token for them.
 */
export class AuthUsersFixture {
  /**
   * The Firebase Auth client to use.
   */
  readonly auth: Auth;

  /**
   * The list of created users.
   */
  readonly users: User[] = [];

  /**
   * The base payload required to form a JWT.
   */
  private readonly jwtBasePayload: Record<string, string>;

  /**
   * Creates a new {@link AuthUsersFixture}.
   */
  constructor() {
    this.auth = getAuth(getDefaultFirebaseApp());

    const projectId = process.env.GOOGLE_CLOUD_PROJECT ?? '';
    this.jwtBasePayload = {
      aud: projectId,
      iss: `https://securetoken.google.com/${projectId}`,
    };
  }

  /**
   * Creates a new user if it doesn't already exist, and generates a corresponding JWT.
   *
   * @param partialUser Properties of a {@link User}.
   * @param tokenOptions Options to pass to `jsonwebtoken` when signing the token.
   * @returns The user itself, and a corresponding JWT.
   */
  async createAuthUserAndToken(
    partialUser: Partial<User> = {},
    tokenOptions: jwt.SignOptions = {},
  ): Promise<{ user: User; token: string }> {
    const { id: userId, ...customClaims } = partialUser;
    const user: User = {
      id: userId ?? uuid.v4(),
      ...customClaims,
    };

    try {
      await this.auth.createUser({ uid: user.id });
    } catch {}

    await this.auth.setCustomUserClaims(user.id, customClaims);

    this.users.push(user);

    const token = jwt.sign(
      { ...this.jwtBasePayload, sub: user.id, ...customClaims },
      null,
      { ...tokenOptions, algorithm: 'none' },
    );

    return { user, token };
  }

  /**
   * Deletes users created by this fixture from the Identity Platform emulator.
   */
  async deleteAll(): Promise<void> {
    await this.auth.deleteUsers(this.users.map((user) => user.id));
  }
}
